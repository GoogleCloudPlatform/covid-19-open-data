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

import uuid
import importlib
import traceback
from pathlib import Path
from functools import partial
from multiprocessing import cpu_count
from typing import Any, Dict, List, Optional, Tuple, Union

import yaml
import requests
from pandas import DataFrame, Int64Dtype

from .anomaly import detect_anomaly_all, detect_stale_columns
from .cast import column_convert
from .concurrent import process_map
from .data_source import DataSource
from .error_logger import ErrorLogger
from .io import read_file, fuzzy_text, export_csv, pbar
from .utils import SRC, CACHE_URL, combine_tables, drop_na_records, filter_output_columns


class DataPipeline(ErrorLogger):
    """
    A pipeline chain is a collection of individual [DataSource]s which produce a full table
    ready for output. This is a very thin wrapper that runs the data pipelines and combines their
    outputs.

    One of the reasons for a dedicated class is to allow for discovery of [DataPipeline] objects
    via reflection, users of this class are encouraged to override its methods if custom processing
    is required.

    A pipeline chain is responsible for loading the auxiliary datasets that are passed to the
    individual pipelines. Pipelines can load data themselves, but if the same auxiliary dataset
    is used by many of them it is more efficient to load it here.
    """

    schema: Dict[str, Any]
    """ Names and corresponding dtypes of output columns """

    data_sources: List[DataSource]
    """ List of data sources (initialized with the appropriate config) executed in order """

    auxiliary_tables: Dict[str, DataFrame] = {"metadata": SRC / "data" / "metadata.csv"}
    """ Auxiliary datasets passed to the pipelines during processing """

    def __init__(
        self,
        schema: Dict[str, type],
        auxiliary: Dict[str, Union[Path, str]],
        data_sources: List[DataSource],
    ):
        super().__init__()
        self.schema = schema
        self.data_sources = data_sources

        # Load the auxiliary tables into memory
        aux = {
            **self.auxiliary_tables,
            **{name: read_file(table) for name, table in auxiliary.items()},
        }

        # Precompute some useful transformations in the auxiliary input files
        aux["metadata"]["match_string_fuzzy"] = aux["metadata"].match_string.apply(fuzzy_text)
        for category in ("country", "subregion1", "subregion2"):
            for suffix in ("code", "name"):
                column = "{}_{}".format(category, suffix)
                aux["metadata"]["{}_fuzzy".format(column)] = aux["metadata"][column].apply(
                    fuzzy_text
                )

        # Set this instance's auxiliary tables to our precomputed tables
        self.auxiliary_tables = aux

    @staticmethod
    def load(name: str):
        config_path = SRC / "pipelines" / name / "config.yaml"
        with open(config_path, "r") as fd:
            config_yaml = yaml.safe_load(fd)
        schema = {
            name: DataPipeline._parse_dtype(dtype) for name, dtype in config_yaml["schema"].items()
        }
        auxiliary = {name: SRC / path for name, path in config_yaml.get("auxiliary", {}).items()}
        data_sources = []
        for pipeline_config in config_yaml["sources"]:
            module_tokens = pipeline_config["name"].split(".")
            class_name = module_tokens[-1]
            module_name = ".".join(module_tokens[:-1])
            module = importlib.import_module(module_name)
            data_sources.append(getattr(module, class_name)(pipeline_config))

        return DataPipeline(schema, auxiliary, data_sources)

    @staticmethod
    def _parse_dtype(dtype_name: str) -> type:
        if dtype_name == "str":
            return str
        if dtype_name == "int":
            return Int64Dtype()
        if dtype_name == "float":
            return float
        raise TypeError(f"Unsupported dtype: {dtype_name}")

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
        for column, dtype in self.schema.items():
            if column not in data:
                data[column] = None
            data[column] = column_convert(data[column], dtype)

        # Filter only output columns and output the sorted data
        return drop_na_records(data[output_columns], ["date", "key"]).sort_values(output_columns)

    @staticmethod
    def _run_wrapper(
        output_folder: Path,
        cache: Dict[str, str],
        aux: Dict[str, DataFrame],
        data_source: DataSource,
    ) -> Optional[DataFrame]:
        """ Workaround necessary for multiprocess pool, which does not accept lambda functions """
        try:
            return data_source.run(output_folder, cache, aux)
        except Exception:
            data_source_name = data_source.__class__.__name__
            data_source.errlog(
                f"Error running data source {data_source_name} with config {data_source.config}"
            )
            traceback.print_exc()
        return None

    def run(
        self,
        pipeline_name: str,
        output_folder: Path,
        process_count: int = cpu_count(),
        verify: str = "simple",
    ) -> DataFrame:
        """
        Main method which executes all the associated [DataSource] objects and combines their
        outputs.

        Arguments:
            pipeline_name: Name of the folder under ./src/pipelines which contains a config.yaml.
            output_folder: Root path of the outputs where "snapshot", "intermediate" and "tables"
                will be created and populated with CSV files.
            progress_count: Maximum number of processes to run in parallel.
            verify: Level of anomaly detection to perform on outputs. Possible values are:
                None, "simple" and "full".
        Returns:
            DataFrame: Processed and combined outputs from all the individual data sources into a
                single table.
        """
        # Read the cache directory from our cloud storage
        try:
            cache = requests.get("{}/sitemap.json".format(CACHE_URL)).json()
        except:
            cache = {}
            self.errlog("Cache unavailable")

        # Get all the pipeline outputs
        # This operation is parallelized but output order is preserved

        # Make a copy of the auxiliary table to prevent modifying it for everyone, but this way
        # we allow for local modification (which might be wanted for optimization purposes)
        aux_copy = {name: df.copy() for name, df in self.auxiliary_tables.items()}

        # Create a function to be used during mapping. The nestedness is an unfortunate outcome of
        # the multiprocessing module's limitations when dealing with lambda functions, coupled with
        # the "sandboxing" we implement to ensure resiliency.
        run_func = partial(DataPipeline._run_wrapper, output_folder, cache, aux_copy)

        # If the process count is less than one, run in series (useful to evaluate performance)
        data_sources_count = len(self.data_sources)
        progress_label = f"Run {pipeline_name} pipeline"
        if process_count <= 1 or data_sources_count <= 1:
            map_func = pbar(
                map(run_func, self.data_sources), total=data_sources_count, desc=progress_label
            )
        else:
            map_func = process_map(run_func, self.data_sources, desc=progress_label)

        # Save all intermediate results (to allow for reprocessing)
        intermediate_outputs = output_folder / "intermediate"
        intermediate_outputs_results: List[Tuple[DataSource, Path]] = []
        for data_source, result in zip(self.data_sources, map_func):
            data_source_class = data_source.__class__
            data_source_config = str(data_source.config)
            source_full_name = f"{data_source_class.__module__}.{data_source_class.__name__}"
            intermediate_name = uuid.uuid5(
                uuid.NAMESPACE_DNS, f"{source_full_name}.{data_source_config}"
            )
            intermediate_file = intermediate_outputs / f"{intermediate_name}.csv"
            intermediate_outputs_results += [(data_source, intermediate_file)]
            if result is not None:
                export_csv(result, intermediate_file)

        # Reload all intermediate results from disk
        # In-memory results are discarded, this ensures reproducibility and allows for data sources
        # to fail since the last successful intermediate result will be used in the combined output
        pipeline_outputs = []
        for data_source, source_output in intermediate_outputs_results:
            try:
                pipeline_outputs += [read_file(source_output, low_memory=False)]
            except Exception as exc:
                data_source_name = data_source.__class__.__name__
                self.errlog(
                    f"Failed to read output for {data_source_name} with config "
                    f"{data_source.config}. Error: {exc}"
                )

        # Get rid of all columns which are not part of the output to speed up data combination
        pipeline_outputs = [
            source_output[filter_output_columns(source_output.columns, self.schema)]
            for source_output in pipeline_outputs
        ]

        # Combine all pipeline outputs into a single DataFrame
        if not pipeline_outputs:
            self.errlog("Empty result for pipeline chain {}".format(pipeline_name))
            data = DataFrame(columns=self.schema.keys())
        else:
            data = combine_tables(pipeline_outputs, ["date", "key"], progress_label=pipeline_name)

        # Return data using the pipeline's output parameters
        data = self.output_table(data)

        # Skip anomaly detection unless requested
        if verify == "simple":

            # Validate that the table looks good
            detect_anomaly_all(self.schema, data, [pipeline_name])

        if verify == "full":

            # Perform stale column detection for each known key
            map_iter = data.key.unique()
            # TODO: convert into a regular function since lambdas cannot be pickled
            map_func = lambda key: detect_stale_columns(
                self.schema, data[data.key == key], (pipeline_name, key)
            )
            progress_label = f"Verify {pipeline_name} pipeline"
            if process_count <= 1 or len(map_iter) <= 1:
                map_func = pbar(map(map_func, map_iter), total=len(map_iter), desc=progress_label)
            else:
                map_func = process_map(map_func, map_iter, desc=progress_label)

            # Consume the results
            _ = list(map_func)

        return data
