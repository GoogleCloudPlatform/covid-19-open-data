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
import time
import uuid
from functools import partial
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import numpy
from pandas import DataFrame

from .error_logger import ErrorLogger
from .cast import isna
from .concurrent import thread_map
from .constants import READ_OPTS
from .io import read_file, fuzzy_text
from .net import download_snapshot
from .time import datetime_isoformat
from .utils import (
    backfill_cumulative_fields_inplace,
    derive_localities,
    filter_columns,
    infer_new_and_total,
    stratify_age_sex_ethnicity,
    table_groupby_sum,
)


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

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__()
        self.config: Dict[str, Any] = config or {}

    def fetch(
        self,
        output_folder: Path,
        cache: Dict[str, str],
        fetch_opts: List[Dict[str, Any]],
        skip_existing: bool = False,
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
        # Download all the resources in parallel using threading
        # Using functions as iterables makes it easier to split the work due to dependent arguments
        map_func = lambda func: func()
        map_iter = [
            partial(
                download_snapshot,
                src["url"],
                output_folder,
                skip_existing=skip_existing,
                logger=self,
                **src.get("opts", {}),
            )
            for src in fetch_opts
        ]
        return {
            fetch_opts[idx].get("name", idx): result
            for idx, result in enumerate(thread_map(map_func, map_iter, desc="Downloading"))
        }

    def _read(self, file_paths: Dict[str, str], **read_opts) -> Dict[str, DataFrame]:
        """ Reads a raw file input path into a DataFrame """
        file_paths = {name: fpath for name, fpath in file_paths.items() if fpath is not None}
        return {name: read_file(fpath, **read_opts) for name, fpath in file_paths.items()}

    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        """ Parses a list of raw data records into a DataFrame. """
        # Some read options are passed as parse_opts
        read_opts = {k: v for k, v in parse_opts.items() if k in READ_OPTS}
        return self.parse_dataframes(self._read(sources, **read_opts), aux, **parse_opts)

    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        """ Parse the inputs into a single output dataframe """
        raise NotImplementedError()

    def merge(
        self, record: Dict[str, Any], aux: Dict[str, DataFrame], keys: Set[str]
    ) -> Optional[str]:
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
                self.log_error(f"Key provided but not found in metadata", record=record)
                return None

        # Start by filtering the auxiliary dataset as much as possible
        for column_prefix in ("country", "subregion1", "subregion2", "locality"):
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

        # Provided match string could be identical to `match_string` (or with simple fuzzy match)
        if match_string is not None:
            aux_match_1 = metadata["match_string_fuzzy"] == match_string
            if sum(aux_match_1) == 1:
                return metadata[aux_match_1].iloc[0]["key"]
            aux_match_2 = metadata["match_string"] == record["match_string"]
            if sum(aux_match_2) == 1:
                return metadata[aux_match_2].iloc[0]["key"]

        # Provided match string could be a subregion code / name
        if match_string is not None:
            for column_prefix in ("subregion1", "subregion2", "locality"):
                # Compare the code as-is
                column = f"{column_prefix}_code"
                aux_match = metadata[column] == record["match_string"]
                if sum(aux_match) == 1:
                    return metadata[aux_match].iloc[0]["key"]

                # Compare the name using fuzzy matching
                column = f"{column_prefix}_name"
                aux_match = metadata[column + "_fuzzy"] == match_string
                if sum(aux_match) == 1:
                    return metadata[aux_match].iloc[0]["key"]

        # Last resort is to match the `match_string` column with a regex from aux
        if match_string is not None:
            aux_mask = ~metadata["match_string"].isna()
            aux_regex = metadata["match_string"][aux_mask].apply(
                lambda x: re.compile(x, re.IGNORECASE)
            )
            for search_string in (match_string, record["match_string"]):
                # pylint: disable=cell-var-from-loop
                aux_match = aux_regex.apply(lambda x: bool(x.match(search_string)))
                if sum(aux_match) == 1:
                    metadata = metadata[aux_mask]
                    return metadata[aux_match].iloc[0]["key"]

            # Log debug info
            self.log_debug(
                "Match info",
                aux_regex=str(aux_regex),
                match_string=match_string,
                record=record,
                metadata=metadata.to_csv(),
            )

        self.log_error(f"No key match found", record=record)
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
        time_start = time.monotonic()
        self.log_info("Starting data source run")

        # Fetch options may not exist if the source decides to do everything within `parse`
        fetch_opts = list(self.config.get("fetch", []))

        # Fetch the data, feeding the cached resources to the fetch step
        data = self.fetch(output_folder, cache, fetch_opts, skip_existing=skip_existing)

        # Make yet another copy of the auxiliary table to avoid affecting future steps in `parse`
        parse_opts = dict(self.config.get("parse", {}))
        data = self.parse(data, {name: df.copy() for name, df in aux.items()}, **parse_opts)

        # Merge expects for null values to be NaN (otherwise grouping does not work as expected)
        data.replace([None], numpy.nan, inplace=True)

        # Get a set with all the known keys so we can use the information during the merge step
        known_keys = set(aux["metadata"]["key"].values)
        merge_func = lambda x: self.merge(x, aux, known_keys)

        # Merging is done record by record, but can be sped up if we build a map first aggregating
        # by the non-temporal fields and only matching the aggregated records with keys
        merge_opts = dict(self.config.get("merge", {}))
        key_merge_columns = [
            col for col in data if col in aux["metadata"].columns and len(data[col].unique()) > 1
        ]
        if not key_merge_columns or (merge_opts and merge_opts.get("serial")):
            data["key"] = data.apply(merge_func, axis=1)

        else:
            # Build a _vec column used to merge the key back from the groups into data
            make_key_vec = lambda x: "|".join([str(x[col]) for col in key_merge_columns])
            data["_vec"] = data[key_merge_columns].apply(make_key_vec, axis=1)

            # Iterate only over the grouped data to merge with the metadata key
            grouped_data = data.groupby("_vec").first().reset_index()
            grouped_data["key"] = grouped_data.apply(merge_func, axis=1)

            # Merge the grouped data which has key back with the original data
            if "key" in data.columns:
                data = data.drop(columns=["key"])
            data = data.merge(grouped_data[["key", "_vec"]], on="_vec").drop(columns=["_vec"])

        # Drop records which have no key merged
        # TODO: log records with missing key somewhere on disk
        data.dropna(subset=["key"], inplace=True)

        # Drop columns which are no longer necessary to identify location
        if not parse_opts.get("keep_metadata"):
            for col_prefix in ("country", "subregion1", "subregion2", "locality"):
                for col_suffix in ("code", "name"):
                    col = f"{col_prefix}_{col_suffix}"
                    if col in data.columns:
                        data.drop(columns=[col], inplace=True)

        # If date is provided, make sure it follows ISO format
        if "date" in data.columns:
            data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%Y-%m-%d"))
            data.dropna(subset=["date"], inplace=True)

        # Filter out data according to the user-provided filter function
        if "query" in self.config:
            data = data.query(self.config["query"]).copy()

        # Get rid of columns according to user-provided config
        if "drop_columns" in self.config:
            data.drop(columns=self.config["drop_columns"], inplace=True)

        # Provide a stratified view of certain key variables
        if any(stratify_column in data.columns for stratify_column in ("age", "sex")):
            data = stratify_age_sex_ethnicity(data)

        # Aggregate records if requested by the config
        if "aggregate" in self.config:

            if "subregion2" in self.config["aggregate"]:
                agg_cols = filter_columns(self.config["aggregate"]["subregion2"], data.columns)
                l2 = data[data["key"].apply(lambda x: len(x.split("_")) == 3)].copy()

                # Remove data from localities
                l2 = l2[~l2["key"].isin(aux["localities"]["locality"].values)]

                # Derive the grouped key by removing the last token
                l2["subregion1_key"] = l2["key"].apply(lambda x: x.rsplit("_", 1)[0])
                group_cols = ["date", "subregion1_key"]
                l1 = table_groupby_sum(l2, group_cols)[group_cols + agg_cols]

                # Remove rows already in data
                l1 = l1[~l1["subregion1_key"].isin(data["key"])]

                data = data.append(l1.rename(columns={"subregion1_key": "key"})).reset_index()

            if "subregion1" in self.config["aggregate"]:
                agg_cols = filter_columns(self.config["aggregate"]["subregion1"], data.columns)
                l1 = data[data["key"].apply(lambda x: len(x.split("_")) == 2)].copy()

                # Remove data from localities
                l1 = l1[~l1["key"].isin(aux["localities"]["locality"].values)]

                # Derive the grouped key by removing the last token
                l1["country_code"] = l1["key"].apply(lambda x: x.rsplit("_", 1)[0])
                group_cols = ["date", "country_code"]
                l0 = table_groupby_sum(l1, group_cols)[group_cols + agg_cols]

                # Remove rows already in data
                l0 = l0[~l0["country_code"].isin(data["key"])]

                data = data.append(l0.rename(columns={"country_code": "key"})).reset_index()

        # Fill with zeroes the requested columns
        # This is useful when we know a data source provides every known data point
        if parse_opts.get("fill_with_zeroes"):
            fill_cols = filter_columns(parse_opts["fill_with_zeroes"], data.columns)
            data[fill_cols] = data[fill_cols].fillna(0)

        # Process each record to add missing cumsum or daily diffs
        data = infer_new_and_total(data)

        if parse_opts.get("backfill"):
            # Backfill cumulative fields with previous entries.
            backfill_cumulative_fields_inplace(data)

        # Derive localities from all regions
        localities = derive_localities(aux["localities"], data)
        if len(localities) > 0:
            data = data.append(localities)

        # Return the final dataframe
        time_elapsed = time.monotonic() - time_start
        self.log_info(f"Data source finished", seconds=time_elapsed, record_count=len(data))
        print(data)
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
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, hash_name))
