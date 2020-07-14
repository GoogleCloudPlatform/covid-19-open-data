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
import json
from pathlib import Path
from unittest import main
from tempfile import TemporaryDirectory
from typing import List

from pandas import DataFrame
from lib.constants import SRC, EXCLUDE_FROM_MAIN_TABLE
from lib.io import read_file, read_lines, export_csv
from lib.pipeline_tools import get_pipelines, get_schema
from .profiled_test_case import ProfiledTestCase
from publish import (
    make_main_table,
    copy_tables,
    convert_csv_to_json_records_fast,
    convert_csv_to_json_records_slow,
)


class TestPublish(ProfiledTestCase):
    def _spot_check_subset(
        self, data: DataFrame, key: str, columns: List[str], first_date: str
    ) -> None:
        subset = data.loc[key, ["date"] + columns]

        # The first date provided has non-null values
        self.assertGreaterEqual(first_date, subset.dropna(subset=columns, how="all").date.min())

        # More than 2/3 of the rows don't have any null values after the first date
        self.assertGreaterEqual(len(subset.dropna()), len(subset[subset.date > first_date]) * 2 / 3)

    def test_make_main_table(self):
        with TemporaryDirectory() as workdir:
            workdir = Path(workdir)

            # Copy all test tables into the temporary directory
            copy_tables(SRC / "test" / "data", workdir)

            # Create the main table
            main_table_path = workdir / "main.csv"
            make_main_table(workdir, main_table_path)
            main_table = read_file(main_table_path)

            # Verify that all columns from all tables exist
            for pipeline in get_pipelines():
                if pipeline.table in EXCLUDE_FROM_MAIN_TABLE:
                    continue
                for column_name in pipeline.schema.keys():
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
            epi_basic = ["new_confirmed", "total_confirmed", "new_deceased", "total_deceased"]

            # Spot check: Country of Andorra
            self._spot_check_subset(main_table, "AD", epi_basic, "2020-03-02")

            # Spot check: State of New South Wales
            self._spot_check_subset(main_table, "AU_NSW", epi_basic, "2020-01-25")

            # Spot check: Alachua County
            self._spot_check_subset(main_table, "US_FL_12001", epi_basic, "2020-03-10")

    def test_convert_csv_to_json_records(self):
        for json_convert_method in (
            convert_csv_to_json_records_fast,
            convert_csv_to_json_records_slow,
        ):
            with TemporaryDirectory() as workdir:
                workdir = Path(workdir)
                schema = get_schema()

                for csv_file in (SRC / "test" / "data").glob("*.csv"):
                    json_output = workdir / csv_file.name.replace("csv", "json")
                    json_convert_method(schema, csv_file, json_output)

                    with json_output.open("r") as fd:
                        json_obj = json.load(fd)
                        json_df = DataFrame(data=json_obj["data"], columns=json_obj["columns"])

                    csv_test_file = workdir / json_output.name.replace("json", "csv")
                    export_csv(json_df, csv_test_file, schema=schema)

                    for line1, line2 in zip(read_lines(csv_file), read_lines(csv_test_file)):
                        self.assertEqual(line1, line2)


if __name__ == "__main__":
    sys.exit(main())
