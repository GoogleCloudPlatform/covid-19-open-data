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

import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, List
from unittest import main

from pandas import DataFrame
from lib.constants import EXCLUDE_FROM_MAIN_TABLE, SRC
from lib.io import read_table, read_lines
from lib.pipeline_tools import get_pipelines, get_schema

from .profiled_test_case import ProfiledTestCase
from publish import copy_tables, convert_tables_to_json, merge_output_tables

# Make the main schema a global variable so we don't have to reload it in every test
SCHEMA = get_schema()


class TestPublish(ProfiledTestCase):
    def _spot_check_subset(
        self, data: DataFrame, key: str, first_date: str, last_date: str
    ) -> None:
        subset = data.loc[key].dropna(axis=1, how="all")
        subset = subset[(subset.date >= first_date) & (subset.date <= last_date)]

        # There are at least 2 non-null columns + date
        self.assertGreaterEqual(len(subset.columns), 3)

        # The first date provided has non-null values
        self.assertGreaterEqual(first_date, subset.dropna(how="all").date.min())

        # Less than half of the rows have null values in any column after the first date
        self.assertGreaterEqual(len(subset.dropna()), len(subset) / 2)

    def _test_make_main_table_helper(self, main_table_path: Path, column_adapter: Dict[str, str]):
        main_table = read_table(main_table_path, schema=SCHEMA)

        # Verify that all columns from all tables exist
        for pipeline in get_pipelines():
            for column_name in pipeline.schema.keys():
                column_name = column_adapter.get(column_name)
                if column_name is not None:
                    self.assertTrue(
                        column_name in main_table.columns,
                        f"Column {column_name} missing from main table",
                    )

        # Main table should follow a lexical sort (outside of header)
        main_table_records = []
        for line in read_lines(main_table_path):
            main_table_records.append(line)
        main_table_records = main_table_records[1:]
        self.assertListEqual(main_table_records, list(sorted(main_table_records)))

        # Make the main table easier to deal with since we optimize for memory usage
        main_table.set_index("key", inplace=True)
        main_table["date"] = main_table["date"].astype(str)

        # Define sets of columns to check
        column_prefixes = ("new", "total")
        columns = [col for col in main_table.columns if col.split("_")[0] in column_prefixes]
        self.assertGreaterEqual(len({col.split("_")[0] for col in columns}), 2)
        main_table = main_table[["date"] + columns]

        # Spot check: Country of Andorra
        self._spot_check_subset(main_table, "AD", "2020-03-02", "2020-09-01")

        # Spot check: State of New South Wales
        self._spot_check_subset(main_table, "AU_NSW", "2020-01-25", "2020-09-01")

        # Spot check: Alachua County
        self._spot_check_subset(main_table, "US_FL_12001", "2020-03-10", "2020-09-01")

    def test_make_main_table(self):
        with TemporaryDirectory() as workdir:
            workdir = Path(workdir)

            # Copy all test tables into the temporary directory
            copy_tables(SRC / "test" / "data", workdir)

            # Create the main table
            main_table_path = workdir / "main.csv"
            merge_output_tables(
                workdir, main_table_path, exclude_table_names=EXCLUDE_FROM_MAIN_TABLE
            )

            self._test_make_main_table_helper(main_table_path, {})

    def test_convert_to_json(self):
        with TemporaryDirectory() as workdir:
            workdir = Path(workdir)

            # Copy all test tables into the temporary directory
            copy_tables(SRC / "test" / "data", workdir)

            # Copy test tables again but under a subpath
            subpath = workdir / "latest"
            subpath.mkdir()
            copy_tables(workdir, subpath)

            # Convert all the tables to JSON under a new path
            jsonpath = workdir / "json"
            jsonpath.mkdir()
            list(convert_tables_to_json(workdir, jsonpath))

            # The JSON files should maintain the same relative path
            for csv_file in workdir.glob("**/*.csv"):
                self.assertTrue((workdir / "json" / f"{csv_file.stem}.json").exists())
                self.assertTrue((workdir / "json" / "latest" / f"{csv_file.stem}.json").exists())


if __name__ == "__main__":
    sys.exit(main())
