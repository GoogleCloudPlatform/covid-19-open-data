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

import csv
import sys
import json
from io import StringIO
from pathlib import Path
from unittest import main
from functools import partial
from typing import Callable, Dict, IO, List

from pandas import DataFrame
from lib.constants import SRC
from lib.io import (
    export_csv,
    line_reader,
    open_file_like,
    read_table,
    temporary_directory,
    temporary_file,
)
from lib.memory_efficient import (
    get_table_columns,
    table_breakout,
    table_cross_product,
    table_join,
    table_grouped_tail,
    table_rename,
    table_sort,
    _convert_csv_to_json_records_fast,
    _convert_csv_to_json_records_slow,
)
from lib.memory_efficient import table_merge as table_merge_mem
from lib.pipeline_tools import get_schema
from lib.utils import agg_last_not_null, pbar
from lib.utils import table_merge as table_merge_pandas
from .profiled_test_case import ProfiledTestCase


# Read the expected dtypes to ensure casting does not throw off test results
SCHEMA = get_schema()


def _make_test_csv_file(raw: str) -> IO:
    buffer = StringIO()

    for line in raw.split("\n"):
        line = line.strip()
        if line and not line.isspace():
            buffer.write(f"{line}\n")

    buffer.flush()
    return buffer


def _compare_tables_equal(test_case: ProfiledTestCase, table1: Path, table2: Path) -> None:
    cols1 = get_table_columns(table1)
    cols2 = get_table_columns(table2)
    test_case.assertEqual(set(cols1), set(cols2))

    # Converting to a CSV in memory sometimes produces out-of-order values
    with open_file_like(table1) as fd1, open_file_like(table2) as fd2:
        records1 = list(line_reader(fd1, skip_empty=True))
        records2 = list(line_reader(fd2, skip_empty=True))
        test_case.assertEqual(len(records1), len(records2))

        reader1 = csv.reader(records1)
        reader2 = csv.reader(records2)
        for record1, record2 in zip(reader1, reader2):
            record1 = {col: val for col, val in zip(cols1, record1)}
            record2 = {col: val for col, val in zip(cols2, record2)}
            test_case.assertEqual(record1, record2)


class TestMemoryEfficient(ProfiledTestCase):
    def _test_join_pair(
        self,
        read_table_: Callable,
        schema: Dict[str, str],
        left: Path,
        right: Path,
        on: List[str],
        how_mem: str,
        how_pandas: str,
    ):
        with temporary_directory() as workdir:
            output_file_1 = workdir / "output.1.csv"
            output_file_2 = workdir / "output.2.csv"

            # Join using our memory efficient method
            table_join(left, right, on, output_file_1, how=how_mem)

            # Join using the pandas method
            pandas_result = read_table_(left).merge(read_table_(right), on=on, how=how_pandas)
            export_csv(pandas_result, output_file_2, schema=schema)

            _compare_tables_equal(self, output_file_1, output_file_2)

    def _test_join_all(self, how_mem: str, how_pandas: str):

        # Create a custom function used to read tables casting to the expected schema
        read_table_ = partial(read_table, schema=SCHEMA, low_memory=False)

        # Test joining the index table with every other table
        left = SRC / "test" / "data" / "index.csv"
        for right in pbar([*(SRC / "test" / "data").glob("*.csv")], leave=False):
            if left.name == right.name:
                continue

            left_columns = get_table_columns(left)
            right_columns = get_table_columns(right)

            if not "date" in right_columns:
                self._test_join_pair(read_table_, SCHEMA, left, right, ["key"], how_mem, how_pandas)

            if "date" in left_columns and not "date" in right_columns:
                self._test_join_pair(read_table_, SCHEMA, left, right, ["key"], how_mem, how_pandas)

            if "date" in left_columns and "date" in right_columns:
                self._test_join_pair(
                    read_table_, SCHEMA, left, right, ["key", "date"], how_mem, how_pandas
                )

    def _test_table_merge(self, how_mem: str, how_pandas: str):
        test_data_1 = DataFrame.from_records(
            [
                {"col1": "a", "col2": "1"},
                {"col1": "a", "col2": "2"},
                {"col1": "b", "col2": "3"},
                {"col1": "b", "col2": "4"},
                {"col1": "c", "col2": "5"},
                {"col1": "c", "col2": "6"},
            ]
        )

        test_data_2 = DataFrame.from_records(
            [
                {"col1": "a", "col3": "foo"},
                {"col1": "b", "col3": "bar"},
                {"col1": "c", "col3": "baz"},
            ]
        )

        test_data_3 = DataFrame.from_records(
            [
                {"col1": "a", "col4": "apple"},
                {"col1": "b", "col4": "banana"},
                {"col1": "c", "col4": "orange"},
            ]
        )

        with temporary_directory() as workdir:

            test_file_1 = workdir / "test.1.csv"
            test_file_2 = workdir / "test.2.csv"
            test_file_3 = workdir / "test.3.csv"

            export_csv(test_data_1, test_file_1)
            export_csv(test_data_2, test_file_2)
            export_csv(test_data_3, test_file_3)

            output_file_1 = workdir / "output.1.csv"
            output_file_2 = workdir / "output.2.csv"

            expected = table_merge_pandas(
                [test_data_1, test_data_2, test_data_3], on=["col1"], how=how_pandas
            )
            export_csv(expected, path=output_file_1)

            table_merge_mem(
                [test_file_1, test_file_2, test_file_3], output_file_2, on=["col1"], how=how_mem
            )

            _compare_tables_equal(self, output_file_1, output_file_2)

    def test_inner_join(self):
        self._test_join_all("inner", "inner")

    def test_outer_join(self):
        self._test_join_all("outer", "left")

    def test_inner_merge(self):
        self._test_table_merge("inner", "inner")

    def test_outer_merge(self):
        self._test_table_merge("outer", "left")

    def test_cross_product(self):
        csv1 = _make_test_csv_file(
            """
            col1,col2
            a,1
            b,2
            c,3
            d,4
            """
        )

        csv2 = _make_test_csv_file(
            """
            col3,col4
            1,a
            2,b
            3,c
            4,d
            """
        )

        expected = _make_test_csv_file(
            """
            col1,col2,col3,col4
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
        )

        with temporary_file() as output_file:
            table_cross_product(csv1, csv2, output_file)
            _compare_tables_equal(self, output_file, expected)

    def test_convert_csv_to_json_records(self):
        for json_convert_method in (
            _convert_csv_to_json_records_fast,
            _convert_csv_to_json_records_slow,
        ):
            with temporary_directory() as workdir:

                for csv_file in pbar([*(SRC / "test" / "data").glob("*.csv")], leave=False):
                    json_output = workdir / csv_file.name.replace("csv", "json")
                    json_convert_method(SCHEMA, csv_file, json_output)

                    with json_output.open("r") as fd:
                        json_obj = json.load(fd)
                        json_df = DataFrame(data=json_obj["data"], columns=json_obj["columns"])

                    csv_test_file = workdir / json_output.name.replace("json", "csv")
                    export_csv(json_df, csv_test_file, schema=SCHEMA)

                    _compare_tables_equal(self, csv_file, csv_test_file)

    def test_table_grouped_tail_synthetic(self):
        test_csv = _make_test_csv_file(
            """
            col1,col2,col3
            a,1,foo
            a,2,bar
            b,1,foo
            b,2,baz
            c,1,foo
            c,2,
            """
        )

        expected = _make_test_csv_file(
            """
            col1,col2,col3
            a,2,bar
            b,2,baz
            c,2,foo
            """
        )

        with temporary_file() as output_file:
            table_grouped_tail(test_csv, output_path=output_file, group_by=["col1"])
            _compare_tables_equal(self, output_file, expected)

    def test_table_grouped_tail_real_data(self):
        with temporary_directory() as workdir:

            for table_path in (SRC / "test" / "data").glob("*.csv"):
                table = read_table(table_path, schema=SCHEMA)
                test_output = workdir / f"latest_{table_path.name}"
                pandas_output = workdir / f"latest_pandas_{table_path.name}"

                # Create the latest slice of the given table
                table_grouped_tail(table_path, test_output, ["key"])

                # Create a latest slice using pandas grouping
                table = table.groupby("key").aggregate(agg_last_not_null).reset_index()
                export_csv(table, path=pandas_output, schema=SCHEMA)

                # Converting to a CSV in memory sometimes produces out-of-order values
                with open_file_like(test_output) as fd1, open_file_like(pandas_output) as fd2:
                    test_result_lines = list(sorted(fd1))
                    pandas_result_lines = list(sorted(fd2))

                self.assertEqual(len(test_result_lines), len(pandas_result_lines))
                for line1, line2 in zip(test_result_lines, pandas_result_lines):
                    self.assertEqual(line1, line2)

    def test_table_rename(self):
        test_csv = _make_test_csv_file(
            """
            col1,col2,col3
            a,1,foo
            b,2,bar
            c,3,foo
            d,4,bar
            """
        )
        expected = _make_test_csv_file(
            """
            cola,colb
            a,1
            b,2
            c,3
            d,4
            """
        )

        with temporary_file() as output_file:
            table_rename(test_csv, output_file, {"col1": "cola", "col2": "colb", "col3": None})
            _compare_tables_equal(self, output_file, expected)

    def test_table_breakout(self):
        test_csv = _make_test_csv_file(
            """
            col1,col2
            foo,1
            foo,2
            bar,3
            bar,4
            baz,5
            baz,6
            """
        )

        expected = {}
        expected["foo"] = _make_test_csv_file(
            """
            col1,col2
            foo,1
            foo,2
            """
        )
        expected["bar"] = _make_test_csv_file(
            """
            col1,col2
            bar,3
            bar,4
            """
        )
        expected["baz"] = _make_test_csv_file(
            """
            col1,col2
            baz,5
            baz,6
            """
        )

        with temporary_directory() as output_folder:
            output_name = "test.csv"
            table_breakout(test_csv, output_folder, "col1", output_name=output_name)

            for breakout_value in ("foo", "bar", "baz"):
                output_path = output_folder / breakout_value / output_name
                _compare_tables_equal(self, output_path, expected[breakout_value])

    def test_table_breakout_unsorted(self):
        test_csv = _make_test_csv_file(
            """
            col1,col2
            foo,1
            foo,2
            bar,3
            bar,4
            baz,5
            foo,6
            """
        )

        with temporary_directory() as output_folder:
            with self.assertRaises(Exception):
                table_breakout(test_csv, output_folder, "col1")

    def test_table_sort(self):
        test_csv = _make_test_csv_file(
            """
            col1,col2,col3
            a,1,foo
            d,4,bar
            c,3,foo
            b,2,bar
            """
        )

        with temporary_directory() as workdir:

            # Sort using the default (first) column
            output_file_1 = workdir / "out.csv"
            table_sort(test_csv, output_file_1)

            test_csv.seek(0)
            output_file_2 = workdir / "pandas.csv"
            read_table(test_csv, file_type="csv").sort_values(["col1"]).to_csv(
                output_file_2, index=False
            )

            _compare_tables_equal(self, output_file_1, output_file_2)

            # Sort by each column in order
            for sort_column in ("col1", "col2", "col3"):

                output_file_1 = workdir / f"1.{sort_column}.csv"
                table_sort(test_csv, output_file_1, [sort_column])

                test_csv.seek(0)
                output_file_2 = workdir / f"2.{sort_column}.csv"
                read_table(test_csv, file_type="csv").sort_values([sort_column]).to_csv(
                    output_file_2, index=False
                )

                _compare_tables_equal(self, output_file_1, output_file_2)


if __name__ == "__main__":
    sys.exit(main())
