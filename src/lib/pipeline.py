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

import re
import uuid
import warnings
import importlib
import traceback
from pathlib import Path
from functools import partial
from multiprocessing import cpu_count
from typing import Any, Dict, List, Optional, Tuple, Union

import yaml
import numpy
import requests
from pandas import DataFrame, Int64Dtype, isna
from tqdm import tqdm

from .anomaly import detect_anomaly_all, detect_stale_columns
from .cast import column_convert
from .concurrent import process_map
from .net import download_snapshot
from .io import read_file, fuzzy_text, export_csv
from .utils import (
    ROOT,
    CACHE_URL,
    combine_tables,
    drop_na_records,
    filter_output_columns,
    infer_new_and_total,
    stratify_age_and_sex,
)


class DataSource:
    """
    Interface for data sources. A data source consists of a series of steps performed in the
    following order:
    1. Fetch: download resources into raw data
    1. Parse: convert raw data to structured format
    1. Merge: associate each record with a known `key`

    The default implementation of a data source includes the following functionality:
    * Fetch: downloads raw data from a list of URLs into ../snapshots folder. See [lib.net].
    * Merge: outputs a key from the auxiliary dataset after performing best-effort matching.

    The merge function provided here is crucial for many sources that use it. The easiest/fastest
    way to merge records is by providing the exact `key` that will match an existing record in the
    [data/metadata.csv] file.
    """

    config: Dict[str, Any]

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__()
        self.config = config or {}

    def fetch(
        self, output_folder: Path, cache: Dict[str, str], fetch_opts: List[Dict[str, Any]]
    ) -> List[str]:
        """
        Downloads the required resources and returns a list of local paths.

        Args:
            output_folder: Root folder where snapshot, intermediate and tables will be placed.
            cache: Map of data sources that are stored in the cache layer (used for daily-only).
            fetch_opts: Additional options defined in the DataPipeline config.yaml.

        Returns:
            List[str]: List of absolute paths where the fetched resources were stored, in the same
                order as they are defined in `config`.
        """
        return [
            download_snapshot(source_config["url"], output_folder, **source_config.get("opts", {}))
            for source_config in fetch_opts
        ]

    def _read(self, file_paths: List[str], **read_opts) -> List[DataFrame]:
        """ Reads a raw file input path into a DataFrame """
        return [read_file(file_path, **read_opts) for file_path in file_paths]

    def parse(self, sources: List[str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        """ Parses a list of raw data records into a DataFrame. """
        # Some read options are passed as parse_opts
        read_opts = {k: v for k, v in parse_opts.items() if k in ("sep",)}
        return self.parse_dataframes(self._read(sources, **read_opts), aux, **parse_opts)

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        """ Parse the inputs into a single output dataframe """
        raise NotImplementedError()

    def merge(self, record: Dict[str, Any], aux: Dict[str, DataFrame]) -> Optional[str]:
        """
        Outputs a key used to merge this record with the datasets.
        The key must be present in the `aux` DataFrame index.
        """
        # Merge only needs the metadata auxiliary data table
        metadata = aux["metadata"]

        # Exact key match might be possible and it's the fastest option
        if "key" in record and not isna(record["key"]):
            if record["key"] in metadata["key"].values:
                return record["key"]
            else:
                warnings.warn("Key provided but not found in metadata: {}".format(record))
                return None

        # Start by filtering the auxiliary dataset as much as possible
        for column_prefix in ("country", "subregion1", "subregion2"):
            for column_suffix in ("code", "name"):
                column = "{}_{}".format(column_prefix, column_suffix)
                if column not in record:
                    continue
                elif isna(record[column]):
                    metadata = metadata[metadata[column].isna()]
                elif record[column]:
                    metadata = metadata[metadata[column] == record[column]]

        # Auxiliary dataset might have a single record left, then we are done
        if len(metadata) == 1:
            return metadata.iloc[0]["key"]

        # Compute a fuzzy version of the record's match string for comparison
        match_string = fuzzy_text(record["match_string"]) if "match_string" in record else None

        # Provided match string could be a subregion code / name
        if match_string is not None:
            for column_prefix in ("subregion1", "subregion2"):
                for column_suffix in ("code", "name"):
                    column = "{}_{}".format(column_prefix, column_suffix)
                    aux_match = metadata[column + "_fuzzy"] == match_string
                    if sum(aux_match) == 1:
                        return metadata[aux_match].iloc[0]["key"]

        # Provided match string could be identical to `match_string` (or with simple fuzzy match)
        if match_string is not None:
            aux_match_1 = metadata["match_string_fuzzy"] == match_string
            if sum(aux_match_1) == 1:
                return metadata[aux_match_1].iloc[0]["key"]
            aux_match_2 = metadata["match_string"] == record["match_string"]
            if sum(aux_match_2) == 1:
                return metadata[aux_match_2].iloc[0]["key"]

        # Last resort is to match the `match_string` column with a regex from aux
        if match_string is not None:
            aux_mask = ~metadata["match_string"].isna()
            aux_regex = metadata["match_string"][aux_mask].apply(
                lambda x: re.compile(x, re.IGNORECASE)
            )
            for search_string in (match_string, record["match_string"]):
                aux_match = aux_regex.apply(lambda x: True if x.match(search_string) else False)
                if sum(aux_match) == 1:
                    metadata = metadata[aux_mask]
                    return metadata[aux_match].iloc[0]["key"]

            # Uncomment when debugging mismatches
            # print(aux_regex)
            # print(match_string)
            # print(record)
            # print(metadata)
            # raise ValueError()

        warnings.warn("No key match found for:\n{}".format(record))
        return None

    def run(
        self,
        output_folder: Path,
        cache: Dict[str, str],
        aux: Dict[str, DataFrame],
        skip_existing: bool = False,
    ) -> DataFrame:
        """
        Executes the fetch, parse and merge steps for this data source.

        Args:
            output_folder: Root folder where snapshot, intermediate and tables will be placed.
            cache: Map of data sources that are stored in the cache layer (used for daily-only).
            aux: Map of auxiliary DataFrames used as part of the processing of this DataSource.
            skip_existing: Flag indicating whether to use the locally stored snapshots if possible.

        Returns:
            DataFrame: Processed data, with columns defined in config.yaml corresponding to the
                DataPipeline that this DataSource is part of.
        """
        data: DataFrame = None

        # Insert skip_existing flag to fetch options if requested
        fetch_opts = self.config.get("fetch", [])
        if skip_existing:
            for opt in fetch_opts:
                opt["opts"] = {**opt.get("opts", {}), "skip_existing": True}

        # Fetch the data, feeding the cached resources to the fetch step
        data = self.fetch(output_folder, cache, fetch_opts)

        # Make yet another copy of the auxiliary table to avoid affecting future steps in `parse`
        parse_opts = self.config.get("parse", {})
        data = self.parse(data, {name: df.copy() for name, df in aux.items()}, **parse_opts)

        # Merge expects for null values to be NaN (otherwise grouping does not work as expected)
        data.replace([None], numpy.nan, inplace=True)

        # Merging is done record by record, but can be sped up if we build a map first aggregating
        # by the non-temporal fields and only matching the aggregated records with keys
        merge_opts = self.config.get("merge", {})
        key_merge_columns = [
            col for col in data if col in aux["metadata"].columns and len(data[col].unique()) > 1
        ]
        if not key_merge_columns or (merge_opts and merge_opts.get("serial")):
            data["key"] = data.apply(lambda x: self.merge(x, aux), axis=1)

        else:
            # "_nan_magic_number" replacement necessary to work around
            # https://github.com/pandas-dev/pandas/issues/3729
            # This issue will be fixed in Pandas 1.1
            _nan_magic_number = -123456789
            grouped_data = (
                data.fillna(_nan_magic_number)
                .groupby(key_merge_columns)
                .first()
                .reset_index()
                .replace([_nan_magic_number], numpy.nan)
            )

            # Build a _vec column used to merge the key back from the groups into data
            make_key_vec = lambda x: "|".join([str(x[col]) for col in key_merge_columns])
            grouped_data["_vec"] = grouped_data.apply(make_key_vec, axis=1)
            data["_vec"] = data.apply(make_key_vec, axis=1)

            # Iterate only over the grouped data to merge with the metadata key
            grouped_data["key"] = grouped_data.apply(lambda x: self.merge(x, aux), axis=1)

            # Merge the grouped data which has key back with the original data
            if "key" in data.columns:
                data = data.drop(columns=["key"])
            data = data.merge(grouped_data[["key", "_vec"]], on="_vec").drop(columns=["_vec"])

        # Drop records which have no key merged
        # TODO: log records with missing key somewhere on disk
        data = data.dropna(subset=["key"])

        # Filter out data according to the user-provided filter function
        if "query" in self.config:
            data = data.query(self.config["query"]).copy()

        # Provide a stratified view of certain key variables
        if any(stratify_column in data.columns for stratify_column in ("age", "sex")):
            data = stratify_age_and_sex(data)

        # Process each record to add missing cumsum or daily diffs
        data = infer_new_and_total(data)

        # Return the final dataframe
        return data


class DataPipeline:
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

    data_sources: List[Tuple[DataSource, Dict[str, Any]]]
    """ List of <data source, option> tuples executed in order """

    auxiliary_tables: Dict[str, Union[Path, str]] = {
        "metadata": ROOT / "src" / "data" / "metadata.csv"
    }
    """ Auxiliary datasets passed to the pipelines during processing """

    def __init__(
        self,
        schema: Dict[str, type],
        auxiliary: Dict[str, Union[Path, str]],
        data_sources: List[Tuple[DataSource, Dict[str, Any]]],
    ):
        super().__init__()
        self.schema = schema
        self.auxiliary_tables = {**self.auxiliary_tables, **auxiliary}
        self.data_sources = data_sources

    @staticmethod
    def load(name: str):
        config_path = ROOT / "src" / "pipelines" / name / "config.yaml"
        with open(config_path, "r") as fd:
            config_yaml = yaml.safe_load(fd)
        schema = {
            name: DataPipeline._parse_dtype(dtype) for name, dtype in config_yaml["schema"].items()
        }
        auxiliary = {name: ROOT / path for name, path in config_yaml.get("auxiliary", {}).items()}
        pipelines = []
        for pipeline_config in config_yaml["sources"]:
            module_tokens = pipeline_config["name"].split(".")
            class_name = module_tokens[-1]
            module_name = ".".join(module_tokens[:-1])
            module = importlib.import_module(module_name)
            pipelines.append(getattr(module, class_name)(pipeline_config))

        return DataPipeline(schema, auxiliary, pipelines)

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
            warnings.warn(
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
        progress: bool = True,
    ) -> DataFrame:
        """
        Main method which executes all the associated [DataSource] objects and combines their
        outputs.
        """
        # Read the cache directory from our cloud storage
        try:
            cache = requests.get("{}/sitemap.json".format(CACHE_URL)).json()
        except:
            cache = {}
            warnings.warn("Cache unavailable")

        # Read the auxiliary input files into memory
        aux = {name: read_file(file_name) for name, file_name in self.auxiliary_tables.items()}

        # Precompute some useful transformations in the auxiliary input files
        aux["metadata"]["match_string_fuzzy"] = aux["metadata"].match_string.apply(fuzzy_text)
        for category in ("country", "subregion1", "subregion2"):
            for suffix in ("code", "name"):
                column = "{}_{}".format(category, suffix)
                aux["metadata"]["{}_fuzzy".format(column)] = aux["metadata"][column].apply(
                    fuzzy_text
                )

        # Get all the pipeline outputs
        # This operation is parallelized but output order is preserved

        # Make a copy of the auxiliary table to prevent modifying it for everyone, but this way
        # we allow for local modification (which might be wanted for optimization purposes)
        aux_copy = {name: df.copy() for name, df in aux.items()}

        # Create a function to be used during mapping. The nestedness is an unfortunate outcome of
        # the multiprocessing module's limitations when dealing with lambda functions, coupled with
        # the "sandboxing" we implement to ensure resiliency.
        run_func = partial(DataPipeline._run_wrapper, output_folder, cache, aux_copy)

        # If the process count is less than one, run in series (useful to evaluate performance)
        data_sources_count = len(self.data_sources)
        progress_label = f"Run {pipeline_name} pipeline"
        if process_count <= 1 or data_sources_count <= 1:
            map_func = tqdm(
                map(run_func, self.data_sources),
                total=data_sources_count,
                desc=progress_label,
                disable=not progress,
            )
        else:
            map_func = process_map(
                run_func, self.data_sources, desc=progress_label, disable=not progress
            )

        # Save all intermediate results (to allow for reprocessing)
        intermediate_outputs = output_folder / "intermediate"
        intermediate_outputs_files = []
        for data_source, result in zip(self.data_sources, map_func):
            data_source_class = data_source.__class__
            data_source_config = str(data_source.config)
            source_full_name = f"{data_source_class.__module__}.{data_source_class.__name__}"
            intermediate_name = uuid.uuid5(
                uuid.NAMESPACE_DNS, f"{source_full_name}.{data_source_config}"
            )
            intermediate_file = intermediate_outputs / f"{intermediate_name}.csv"
            intermediate_outputs_files += [intermediate_file]
            if result is not None:
                export_csv(result, intermediate_file)

        # Reload all intermediate results from disk
        # In-memory results are discarded, this ensures reproducibility and allows for data sources
        # to fail since the last successful intermediate result will be used in the combined output
        pipeline_outputs = []
        for source_output in intermediate_outputs_files:
            try:
                pipeline_outputs += [read_file(source_output)]
            except Exception as exc:
                warnings.warn(f"Failed to read intermediate file {source_output}. Error: {exc}")

        # Get rid of all columns which are not part of the output to speed up data combination
        pipeline_outputs = [
            source_output[filter_output_columns(source_output.columns, self.schema)]
            for source_output in pipeline_outputs
        ]

        # Combine all pipeline outputs into a single DataFrame
        if not pipeline_outputs:
            warnings.warn("Empty result for pipeline chain {}".format(pipeline_name))
            data = DataFrame(columns=self.schema.keys())
        else:
            progress_label = pipeline_name if progress else None
            data = combine_tables(pipeline_outputs, ["date", "key"], progress_label=progress_label)

        # Return data using the pipeline's output parameters
        data = self.output_table(data)

        # Skip anomaly detection unless requested
        if verify == "simple":

            # Validate that the table looks good
            detect_anomaly_all(self.schema, data, [pipeline_name])

        if verify == "full":

            # Perform stale column detection for each known key
            map_iter = data.key.unique()
            map_func = lambda key: detect_stale_columns(
                self.schema, data[data.key == key], (pipeline_name, key)
            )
            progress_label = f"Verify {pipeline_name} pipeline"
            if process_count <= 1 or len(map_iter) <= 1:
                map_func = tqdm(
                    map(map_func, map_iter),
                    total=len(map_iter),
                    desc=progress_label,
                    disable=not progress,
                )
            else:
                map_func = process_map(
                    map_func, map_iter, desc=progress_label, disable=not progress
                )

            # Show progress as the results arrive if requested
            if progress:
                map_func = tqdm(
                    map_func, total=len(map_iter), desc=f"Verify {pipeline_name} pipeline"
                )

            # Consume the results
            _ = list(map_func)

        return data
