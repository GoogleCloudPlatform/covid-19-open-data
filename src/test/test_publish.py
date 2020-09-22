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
from unittest import main
from tempfile import TemporaryDirectory
from typing import List

from pandas import DataFrame
from lib.constants import SRC, OUTPUT_COLUMN_ADAPTER
from lib.io import read_table, read_lines
from lib.pipeline_tools import get_pipelines, get_schema
from .profiled_test_case import ProfiledTestCase
from publish import make_main_table, convert_tables_to_json, publish_global_tables

# Make the main schema a global variable so we don't have to reload it in every test
SCHEMA = get_schema()


class TestPublish(ProfiledTestCase):
    def _spot_check_subset(
        self, data: DataFrame, key: str, columns: List[str], first_date: str, last_date: str
    ) -> None:
        subset = data.loc[key, ["date"] + columns]
        subset = subset[(subset.date >= first_date) & (subset.date <= last_date)]

        # The first date provided has non-null values
        self.assertGreaterEqual(first_date, subset.dropna(subset=columns, how="all").date.min())

        # Less than half of the rows have null values in any column after the first date
        self.assertGreaterEqual(len(subset.dropna()), len(subset) / 2)

    def test_make_main_table(self):
        with TemporaryDirectory() as workdir:
            workdir = Path(workdir)

            # Copy all test tables into the temporary directory
            publish_global_tables(SRC / "test" / "data", workdir)

            # Create the main table
            main_table_path = workdir / "main.csv"
            make_main_table(
                workdir, main_table_path, location_key="location_key", exclude_tables=["main"]
            )
            main_table = read_table(main_table_path, schema=SCHEMA)

            # Verify that all columns from all tables exist
            for pipeline in get_pipelines():
                for column_name in pipeline.schema.keys():
                    column_name = OUTPUT_COLUMN_ADAPTER.get(column_name)
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
            main_table.set_index("location_key", inplace=True)
            main_table["date"] = main_table["date"].astype(str)

            # Define sets of columns to check
            epi_basic = [
                "new_confirmed",
                "cumulative_confirmed",
                "new_deceased",
                "cumulative_deceased",
            ]

            # Spot check: Country of Andorra
            self._spot_check_subset(main_table, "AD", epi_basic, "2020-03-02", "2020-09-01")

            # Spot check: State of New South Wales
            self._spot_check_subset(main_table, "AU_NSW", epi_basic, "2020-01-25", "2020-09-01")

            # Spot check: Alachua County
            self._spot_check_subset(
                main_table, "US_FL_12001", epi_basic, "2020-03-10", "2020-09-01"
            )

    def test_convert_to_json(self):
        with TemporaryDirectory() as workdir:
            workdir = Path(workdir)

            # Copy all test tables into the temporary directory
            publish_global_tables(SRC / "test" / "data", workdir)

            # Copy test tables again but under a subpath
            subpath = workdir / "latest"
            subpath.mkdir()
            publish_global_tables(workdir, subpath)

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
