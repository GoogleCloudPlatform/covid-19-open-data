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
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy
from pandas import DataFrame, concat, isna

from .error_logger import ErrorLogger
from .io import read_file, fuzzy_text
from .net import download_snapshot
from .time import datetime_isoformat
from .utils import derive_localities, infer_new_and_total, stratify_age_sex_ethnicity


class DataSource(ErrorLogger):
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
    ) -> Dict[str, str]:
        """
        Downloads the required resources and returns a dictionary of <name, local path>.

        Args:
            output_folder: Root folder where snapshot, intermediate and tables will be placed.
            cache: Map of data sources that are stored in the cache layer (used for daily-only).
            fetch_opts: Additional options defined in the DataPipeline config.yaml.

        Returns:
            Dict[str, str]: Dict of absolute paths where the fetched resources were stored, using
                the "name" as the key if it is defined in `config`, otherwise the keys are ordinal
                numbers based on the order of the URLs.
        """
        return {
            source_config.get("name", idx): download_snapshot(
                source_config["url"], output_folder, **source_config.get("opts", {})
            )
            for idx, source_config in enumerate(fetch_opts)
        }

    def _read(self, file_paths: Dict[str, str], **read_opts) -> List[DataFrame]:
        """ Reads a raw file input path into a DataFrame """
        return {name: read_file(file_path, **read_opts) for name, file_path in file_paths.items()}

    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        """ Parses a list of raw data records into a DataFrame. """
        # Some read options are passed as parse_opts
        read_opts = {
            k: v
            for k, v in parse_opts.items()
            if k
            in (
                "sep",
                "encoding",
                "low_memory",
                "file_name",
                "sheet_name",
                "usecols",
                "error_bad_lines",
            )
        }
        return self.parse_dataframes(self._read(sources, **read_opts), aux, **parse_opts)

    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
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

        # If date is provided, make sure it follows ISO format
        if "date" in record:
            date = datetime_isoformat(record["date"], "%Y-%m-%d")
            if date is None:
                self.errlog(f"Invalid date:\n{record}")
                return None
            else:
                # Re-set the record's date to make sure it's the appropriate type
                record["date"] = date

        # Exact key match might be possible and it's the fastest option
        if "key" in record and not isna(record["key"]):
            if record["key"] in metadata["key"].values:
                return record["key"]
            else:
                self.errlog(f"Key provided but not found in metadata:\n{record}")
                return None

        # Localities should only be matched using a key directly
        if "locality_code" in metadata.columns:
            metadata = metadata[metadata["locality_code"].isna()]

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

        self.errlog(f"No key match found for:\n{record}")
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

        # Derive localities from all regions
        data = concat([data, derive_localities(aux["localities"], data)])

        # Provide a stratified view of certain key variables
        if any(stratify_column in data.columns for stratify_column in ("age", "sex")):
            data = stratify_age_sex_ethnicity(data)

        # Process each record to add missing cumsum or daily diffs
        data = infer_new_and_total(data)

        # Return the final dataframe
        return data

    def uuid(self, table_name: str) -> str:
        """
        Generates a deterministic identifier based on this data source's class and configuration.

        Returns:
            str: A uuid which can be used to uniquely identify this data source + config
        """
        data_source_class = self.__class__
        configs = self.config.items()
        config_invariant = ("test", "automation")
        data_source_config = str({key: val for key, val in configs if key not in config_invariant})
        source_full_name = f"{data_source_class.__module__}.{data_source_class.__name__}"
        hash_name = f"{table_name}.{source_full_name}.{data_source_config}"
        return uuid.uuid5(uuid.NAMESPACE_DNS, hash_name)
