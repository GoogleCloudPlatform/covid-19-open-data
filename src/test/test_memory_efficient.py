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
from functools import partial
from tempfile import TemporaryDirectory
from typing import Callable, Dict

from pandas import DataFrame
from lib.constants import SRC
from lib.io import read_lines, read_table, export_csv
from lib.memory_efficient import (
    table_cross_product,
    table_join,
    table_group_tail,
    _convert_csv_to_json_records_fast,
    _convert_csv_to_json_records_slow,
)
from lib.pipeline_tools import get_schema
from lib.utils import pbar
from .profiled_test_case import ProfiledTestCase


# Read the expected dtypes to ensure casting does not throw off test results
SCHEMA = get_schema()


class TestTableJoins(ProfiledTestCase):
    def _test_join_pair(
        self,
        read_table_: Callable,
        schema: Dict[str, str],
        left: Path,
        right: Path,
        on: str,
        how: str,
    ):
        with TemporaryDirectory() as workdir:
            workdir = Path(workdir)
            tmpfile = workdir / "tmpfile.csv"

            table_join(left, right, on, tmpfile, how=how)
            test_result = export_csv(read_table_(tmpfile), schema=schema)
            pandas_how = how.replace("outer", "left")
            pandas_result = export_csv(
                read_table_(left).merge(read_table_(right), how=pandas_how), schema=schema
            )

            for line1, line2 in zip(test_result.split("\n"), pandas_result.split("\n")):
                self.assertEqual(line1, line2)

    def _test_join_all(self, how: str):

        # Create a custom function used to read tables casting to the expected schema
        read_table_ = partial(read_table, schema=SCHEMA, low_memory=False)

        for left in pbar([*(SRC / "test" / "data").glob("*.csv")], leave=False):
            for right in pbar([*(SRC / "test" / "data").glob("*.csv")], leave=False):
                if left.name == right.name:
                    continue

                left_columns = read_table_(left).columns
                right_columns = read_table_(right).columns

                if not "date" in right_columns:
                    self._test_join_pair(read_table_, SCHEMA, left, right, ["key"], how)

                if "date" in left_columns and not "date" in right_columns:
                    self._test_join_pair(read_table_, SCHEMA, left, right, ["key"], how)

                if "date" in left_columns and "date" in right_columns:
                    self._test_join_pair(read_table_, SCHEMA, left, right, ["key", "date"], how)

    def test_inner_join(self):
        self._test_join_all("inner")

    def test_outer_join(self):
        self._test_join_all("outer")

    def test_cross_product(self):
        csv1 = """col1,col2
        a,1
        b,2
        c,3
        d,4
        """

        csv2 = """col3,col4
        1,a
        2,b
        3,c
        4,d
        """

        expected = """col1,col2,col3,col4
        a,1,1,a
        a,1,2,b
        a,1,3,c
        a,1,4,d
        b,2,1,a
        b,2,2,b
        b,2,3,c
        b,2,4,d
        c,3,1,a
        c,3,2,b
        c,3,3,c
        c,3,4,d
        d,4,1,a
        d,4,2,b
        d,4,3,c
        d,4,4,d
        """

        with TemporaryDirectory() as workdir:
            workdir = Path(workdir)
            with open(workdir / "1.csv", "w") as fd:
                for line in csv1.split("\n"):
                    if not line.isspace():
                        fd.write(f"{line.strip()}\n")
            with open(workdir / "2.csv", "w") as fd:
                for line in csv2.split("\n"):
                    if not line.isspace():
                        fd.write(f"{line.strip()}\n")

            output_file = workdir / "out.csv"
            table_cross_product(workdir / "1.csv", workdir / "2.csv", output_file)

            for line1, line2 in zip(expected.split("\n"), read_lines(output_file)):
                self.assertEqual(line1.strip(), line2.strip())

    def test_convert_csv_to_json_records(self):
        for json_convert_method in (
            _convert_csv_to_json_records_fast,
            _convert_csv_to_json_records_slow,
        ):
            schema = get_schema()

            with TemporaryDirectory() as workdir:
                workdir = Path(workdir)

                for csv_file in pbar([*(SRC / "test" / "data").glob("*.csv")], leave=False):
                    json_output = workdir / csv_file.name.replace("csv", "json")
                    json_convert_method(schema, csv_file, json_output)

                    with json_output.open("r") as fd:
                        json_obj = json.load(fd)
                        json_df = DataFrame(data=json_obj["data"], columns=json_obj["columns"])

                    csv_test_file = workdir / json_output.name.replace("json", "csv")
                    export_csv(json_df, csv_test_file, schema=schema)

                    for line1, line2 in zip(read_lines(csv_file), read_lines(csv_test_file)):
                        self.assertEqual(line1, line2)

    def test_table_group_tail(self):
        with TemporaryDirectory() as workdir:
            workdir = Path(workdir)
            schema = get_schema()

            for table_path in (SRC / "test" / "data").glob("*.csv"):
                table = read_table(table_path, schema=schema)
                output_path = workdir / f"latest_{table_path.name}"

                # Create the latest slice of the given table
                table_group_tail(table_path, output_path)

                # Read the created latest slice
                latest_ours = read_table(output_path, schema=schema)

                # Create a latest slice using pandas grouping
                if "total_confirmed" in table.columns:
                    table = table.dropna(subset=["total_confirmed"])
                latest_pandas = table.groupby(["key"]).tail(1)

                self.assertEqual(
                    export_csv(latest_ours, schema=schema), export_csv(latest_pandas, schema=schema)
                )


if __name__ == "__main__":
    sys.exit(main())
