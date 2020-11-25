# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import importlib
import traceback
from pathlib import Path
from functools import partial
from multiprocessing import cpu_count
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import yaml
import requests
from pandas import DataFrame, concat

from .anomaly import detect_anomaly_all, detect_stale_columns
from .cast import column_converters
from .constants import SRC, CACHE_URL
from .concurrent import process_map
from .data_source import DataSource
from .error_logger import ErrorLogger
from .io import read_file, read_table, fuzzy_text, export_csv, parse_dtype, pbar
from .lazy_property import lazy_property
from .utils import combine_tables, drop_na_records, filter_output_columns


class DataPipeline(ErrorLogger):
    """
    A data pipeline is a collection of individual [DataSource]s which produce a full table ready
    for output. This is a very thin wrapper that pulls the data sources and combines their outputs.

    A data pipeline is responsible for loading the auxiliary datasets that are passed to the
    individual data sources. DataSource objects can load data themselves, but if the same auxiliary
    dataset is used by many of them, then it is more efficient to load it here.
    """

    def __init__(
        self,
        name: str,
        schema: Dict[str, type],
        auxiliary: Dict[str, Union[Path, str]],
        data_sources: List[DataSource],
    ):
        """
        Arguments:
            name: The name of this module
            table: The name of the table corresponding to this pipeline
            schema: Names and corresponding dtypes of output columns.
            auxiliary: Auxiliary datasets passed to the pipelines during processing
            data_sources: List of data sources (initialized with the appropriate config) executed
                in order.
        """
        super().__init__()
        self.name: str = name
        self.schema: Dict[str, Any] = schema
        self.data_sources: List[DataSource] = data_sources
        self.table: str = name.replace("_", "-")
        self._auxiliary: Dict[str, Union[Path, str]] = auxiliary

    @lazy_property
    def auxiliary_tables(self):
        """ Auxiliary datasets passed to the pipelines during processing """

        # Metadata table can be overridden but must always be present
        auxiliary = {"metadata": SRC / "data" / "metadata.csv", **self._auxiliary}

        # Load the auxiliary tables into memory
        aux = {name: read_file(table) for name, table in auxiliary.items()}

        # Precompute some useful transformations in the auxiliary input files
        aux["metadata"]["match_string_fuzzy"] = aux["metadata"].match_string.apply(fuzzy_text)
        for column in ("subregion1", "subregion2", "locality"):
            # Apply fuzzy comparison for name fields
            aux["metadata"][f"{column}_name_fuzzy"] = aux["metadata"][f"{column}_name"].apply(
                fuzzy_text
            )

        return aux

    @staticmethod
    def load(name: str) -> "DataPipeline":
        """
        Load a data pipeline by reading its configuration at the expected path from the given name.

        Arguments:
            name: Name of the data pipeline, which is the same as the name of the output table but
                replacing underscores (`_`) with dashes (`-`).
        Returns:
            DataPipeline: The DataPipeline object corresponding to the input name.
        """
        # Read config from the yaml file
        config_path = SRC / "pipelines" / name / "config.yaml"
        with open(config_path, "r") as fd:
            config_yaml = yaml.safe_load(fd)

        # The pipeline's schema and auxiliary tables are part of the config
        schema = {name: parse_dtype(dtype) for name, dtype in config_yaml["schema"].items()}
        auxiliary = {name: SRC / path for name, path in config_yaml.get("auxiliary", {}).items()}

        data_sources = []
        for idx, source_config in enumerate(config_yaml["sources"]):
            # Add the job group to all configs
            automation_config = source_config.get("automation", {})
            source_config["automation"] = automation_config
            source_config["automation"]["job_group"] = automation_config.get("job_group", str(idx))

            # Use reflection to create an instance of the corresponding DataSource class
            module_tokens = source_config["name"].split(".")
            class_name = module_tokens[-1]
            module_name = ".".join(module_tokens[:-1])
            module = importlib.import_module(module_name)

            # Create the DataSource class with the appropriate config
            data_sources.append(getattr(module, class_name)(source_config))

        return DataPipeline(name, schema, auxiliary, data_sources)

    def output_table(self, data: DataFrame) -> DataFrame:
        """
        This function performs the following operations:
        1. Filters out columns not in the output schema
        2. Converts each column to the appropriate type
        3. Sorts the values based on the column order
        4. Outputs the resulting data
        """
        output_columns = list(self.schema.keys())

        # Make sure all columns are present and have the appropriate type
        for column, converter in column_converters(self.schema).items():
            if column not in data:
                data[column] = None
            data[column] = data[column].apply(converter)

        # Filter only output columns and output the sorted data
        return drop_na_records(data[output_columns], ["date", "key"]).sort_values(output_columns)

    @staticmethod
    def _run_wrapper(
        output_folder: Path,
        cache: Dict[str, str],
        aux: Dict[str, DataFrame],
        data_source: DataSource,
        **source_opts,
    ) -> Optional[DataFrame]:
        """ Workaround necessary for multiprocess pool, which does not accept lambda functions """
        try:
            return data_source.run(output_folder, cache, aux, **source_opts)
        except Exception:
            data_source_name = data_source.__class__.__name__
            data_source.log_error(
                "Error running data source.",
                source_name=data_source_name,
                config=data_source.config,
                traceback=traceback.format_exc(),
            )
        return None

    def parse(
        self, output_folder: Path, process_count: int = None, **source_opts
    ) -> Iterable[Tuple[DataSource, DataFrame]]:
        """
        Performs the fetch and parse steps for each of the data sources in this pipeline.

        Arguments:
            output_folder: Root path of the outputs where "snapshot", "intermediate" and "tables"
                will be created and populated with CSV files.
            process_count: Maximum number of processes to run in parallel.
        Returns:
            Iterable[Tuple[DataSource, DataFrame]]: Pairs of <data source, results> for each data
                source, where the results are the output of `DataSource.parse()`.
        """

        # Read the cache directory from our cloud storage
        try:
            cache = requests.get("{}/sitemap.json".format(CACHE_URL)).json()
        except:
            cache = {}
            self.log_error("Cache unavailable")

        # Make a copy of the auxiliary table to prevent modifying it for everyone, but this way
        # we allow for local modification (which might be wanted for optimization purposes)
        aux_copy = {name: df.copy() for name, df in self.auxiliary_tables.items()}

        # Create a function to be used during mapping. The nestedness is an unfortunate outcome of
        # the multiprocessing module's limitations when dealing with lambda functions, coupled with
        # the "sandboxing" we implement to ensure resiliency.
        map_func = partial(DataPipeline._run_wrapper, output_folder, cache, aux_copy, **source_opts)

        # Default to using as many processes as CPUs
        if process_count is None:
            process_count = cpu_count()

        # Used to display progress during processing
        data_sources_count = len(self.data_sources)
        progress_label = f"Run {self.name} pipeline"
        map_opts = dict(total=data_sources_count, desc=progress_label)

        # If the process count is less than one, run in series (useful to evaluate performance)
        if process_count <= 1 or data_sources_count <= 1:
            map_result = pbar(map(map_func, self.data_sources), **map_opts)
        else:
            map_opts.update(dict(max_workers=process_count))
            map_result = process_map(map_func, self.data_sources, **map_opts)

        # Get all the pipeline outputs
        # This operation is parallelized but output order is preserved
        yield from zip(self.data_sources, map_result)

    def _save_intermediate_results(
        self,
        intermediate_folder: Path,
        intermediate_results: Iterable[Tuple[DataSource, DataFrame]],
    ) -> None:
        for data_source, result in intermediate_results:
            if result is not None:
                self.log_info(f"Exporting results from {data_source.__class__.__name__}")
                file_name = f"{data_source.uuid(self.table)}.csv"
                export_csv(result, intermediate_folder / file_name, schema=self.schema)
            else:
                data_source_name = data_source.__class__.__name__
                self.log_error(
                    "No output while saving intermediate results",
                    source_name=data_source_name,
                    source_config=data_source.config,
                )

    def _load_intermediate_results(
        self, intermediate_folder: Path
    ) -> Iterable[Tuple[DataSource, DataFrame]]:

        for data_source in self.data_sources:
            intermediate_path = intermediate_folder / f"{data_source.uuid(self.table)}.csv"
            try:
                yield (data_source, read_table(intermediate_path, schema=self.schema))
            except Exception as exc:
                data_source_name = data_source.__class__.__name__
                self.log_error(
                    "Failed to load intermediate output",
                    source_name=data_source_name,
                    source_config=data_source.config,
                    exception=exc,
                )

    def _split_combine_inputs(
        self, intermediate_results: Iterable[Tuple[DataSource, DataFrame]], combine_chunk_size: int
    ) -> List[DataFrame]:
        combine_inputs = concat(intermediate_results)
        combine_inputs_columns = combine_inputs.columns
        idx_col = "date" if "date" in combine_inputs.columns else "key"
        combine_inputs.dropna(subset=[idx_col], inplace=True)
        combine_inputs.set_index(idx_col, inplace=True)
        index = combine_inputs.index.unique()

        new_chunk = lambda: DataFrame(columns=combine_inputs_columns).set_index(idx_col)
        current_chunk = new_chunk()
        current_chunk_len = 0

        for idx in sorted(index):
            idx_chunk = combine_inputs.loc[idx]
            idx_chunk_len = len(idx_chunk)

            if idx_chunk_len > 0 and current_chunk_len + idx_chunk_len > combine_chunk_size:
                current_chunk_len = 0
                yield current_chunk.reset_index()
                current_chunk = new_chunk()

            current_chunk_len += idx_chunk_len
            current_chunk = current_chunk.append(idx_chunk)

        if len(current_chunk) > 0:
            yield current_chunk.reset_index()

    def combine(
        self,
        intermediate_results: Iterable[Tuple[DataSource, DataFrame]],
        process_count: int = None,
        combine_chunk_size: int = 2 ** 16,
    ) -> DataFrame:
        """
        Combine all the provided intermediate results into a single DataFrame, giving preference to
        values coming from the latter results.

        Arguments:
            intermediate_results: collection of results from individual data sources.
            process_count: Maximum number of processes to run in parallel.
            combine_chunk_size: Maximum number of rows to send to combine grouping function.
        """

        # Default to using as many processes as CPUs
        if process_count is None:
            process_count = cpu_count()

        # Combine all intermediate outputs into a single DataFrame
        if not intermediate_results:
            self.log_error("Empty result for data pipeline {}".format(self.name))
            pipeline_output = [DataFrame(columns=self.schema.keys())]
        else:

            # Get rid of all columns which are not part of the output to speed up data combination
            intermediate_tables = (
                result[filter_output_columns(result.columns, self.schema)]
                for _, result in intermediate_results
            )

            # Parallelize combine step into N processes
            combine_inputs = self._split_combine_inputs(intermediate_tables, combine_chunk_size)
            map_iter = [[chunk] for chunk in combine_inputs]
            map_func = partial(combine_tables, index=["date", "key"])
            map_opts = dict(max_workers=process_count, desc=f"Combining {self.name} outputs")
            pipeline_output = process_map(map_func, map_iter, **map_opts)
            del combine_inputs

        # Return data using the pipeline's output parameters
        map_opts = dict(max_workers=process_count, desc=f"Cleaning {self.name} outputs")
        return concat(process_map(self.output_table, pipeline_output, **map_opts))

    def verify(
        self, pipeline_output: DataFrame, level: str = "simple", process_count: int = cpu_count()
    ) -> DataFrame:
        """
        Perform verification tasks on the data pipeline combined outputs.

        Arguments:
            pipeline_output: Output of `DataPipeline.combine()`.
            process_count: Maximum number of processes to run in parallel.
            verify_level: Level of anomaly detection to perform on outputs. Possible values are:
                None, "simple" and "full".
        Returns:
            DataFrame: same as `pipeline_output`.

        """

        # Skip anomaly detection unless requested
        if level == "simple":

            # Validate that the table looks good
            detect_anomaly_all(self.schema, pipeline_output, [self.name])

        if level == "full":

            # Perform stale column detection for each known key
            map_iter = pipeline_output.key.unique()
            # TODO: convert into a regular function since lambdas cannot be pickled
            map_func = lambda key: detect_stale_columns(
                self.schema, pipeline_output[pipeline_output.key == key], (self.name, key)
            )
            progress_label = f"Verify {self.name} pipeline"
            if process_count <= 1 or len(map_iter) <= 1:
                map_func = pbar(map(map_func, map_iter), total=len(map_iter), desc=progress_label)
            else:
                map_func = process_map(map_func, map_iter, desc=progress_label)

            # Consume the results
            _ = list(map_func)

        return pipeline_output

    def run(
        self,
        output_folder: Path,
        process_count: int = cpu_count(),
        verify_level: str = "simple",
        **source_opts,
    ) -> DataFrame:
        """
        Main method which executes all the associated [DataSource] objects and combines their
        outputs.

        Arguments:
            output_folder: Root path of the outputs where "snapshot", "intermediate" and "tables"
                will be created and populated with CSV files.
            process_count: Maximum number of processes to run in parallel.
            verify_level: Level of anomaly detection to perform on outputs. Possible values are:
                None, "simple" and "full".
            source_opts: Options to relay to the DataSource.run() method.
        Returns:
            DataFrame: Processed and combined outputs from all the individual data sources into a
                single table.
        """
        # TODO: break out fetch & parse steps
        intermediate_results = self.parse(output_folder, process_count=process_count, **source_opts)

        # Save all intermediate results (to allow for reprocessing)
        intermediate_folder = output_folder / "intermediate"
        self._save_intermediate_results(intermediate_folder, intermediate_results)

        # Re-load all intermediate results
        intermediate_results = self._load_intermediate_results(intermediate_folder)

        # Combine all intermediate results into a single dataframe
        # NOTE: Limit the number of processes to avoid OOM in big datasets
        pipeline_output = self.combine(intermediate_results, process_count=min(process_count, 4))

        # Perform anomaly detection on the combined outputs
        pipeline_output = self.verify(
            pipeline_output, level=verify_level, process_count=process_count
        )

        return pipeline_output
