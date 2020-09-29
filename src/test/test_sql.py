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
from sqlite3.dbapi2 import Connection
from tempfile import TemporaryDirectory
from typing import Any, Dict, Iterator

from pandas import DataFrame
from lib.constants import SRC
from lib.pipeline_tools import get_schema
from lib.memory_efficient import table_rename, get_table_columns
from lib.sql import (
    _safe_column_name,
    _safe_table_name,
    create_sqlite_database,
    table_create,
    table_import_from_file,
    table_import_from_records,
    table_select_all,
    table_export_csv,
)

from lib.sql import table_join as table_join_sql
from lib.sql import table_merge as table_merge_sql
from lib.utils import table_merge as table_merge_pandas

from .profiled_test_case import ProfiledTestCase
from .test_memory_efficient import _compare_tables_equal

# Make the main schema a global variable so we don't have to reload it in every test
SCHEMA = get_schema()


def _dataframe_records_iterator(df: DataFrame) -> Iterator[Dict[str, Any]]:
    return (row.to_dict() for _, row in df.iterrows())


class TestSql(ProfiledTestCase):
    def _compare_dataframes_equal(self, df1: DataFrame, df2: DataFrame) -> None:
        records1 = list(_dataframe_records_iterator(df1))
        records2 = list(_dataframe_records_iterator(df2))
        self.assertEqual(len(records1), len(records2))
        for record1, record2 in zip(records1, records2):
            self.assertDictEqual(record1, record2)

    def _check_table_not_empty(self, conn: Connection, table_name: str) -> None:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {_safe_table_name(table_name)}")
        records = cursor.fetchall()
        self.assertGreaterEqual(len(records), 1)
        cursor.close()

    def test_create_sqlite_db_file(self):
        with TemporaryDirectory() as workdir:
            workdir = Path(workdir)

            sqlite_file = workdir / "tmp.sqlite"
            with create_sqlite_database(db_file=sqlite_file) as conn:
                self.assertEqual(conn.execute("SELECT 1").fetchone()[0], 1)

    def test_create_sqlite_db_in_memory(self):
        with create_sqlite_database() as conn:
            self.assertEqual(conn.execute("SELECT 1").fetchone()[0], 1)

    def test_table_file_reimport(self):
        with TemporaryDirectory() as workdir:
            workdir = Path(workdir)

            sqlite_file = workdir / "tmp.sqlite"
            tables_folder = SRC / "test" / "data"

            # Verify that all tables were imported
            with create_sqlite_database(db_file=sqlite_file) as conn:
                for table_path in tables_folder.glob("*.csv"):
                    table_name = _safe_table_name(table_path.stem)
                    table_import_from_file(conn, table_path, table_name=table_name)
                    self._check_table_not_empty(conn, table_name)

                    # Dirty hack used to compare appropriate column names. Ideally this would be
                    # handled by the SQL module, which should convert the table and column names to
                    # whatever they were prior to sanitizing them.
                    temp_file_path_1 = workdir / f"{table_name}.1.csv"
                    column_adapter = {
                        col: _safe_column_name(col).replace("[", "").replace("]", "")
                        for col in get_table_columns(table_path)
                    }
                    table_rename(table_path, temp_file_path_1, column_adapter)

                    temp_file_path_2 = workdir / f"{table_name}.2.csv"
                    table_export_csv(conn, table_name, temp_file_path_2)
                    _compare_tables_equal(self, temp_file_path_1, temp_file_path_2)

    def _test_table_join(self, how_sqlite: str, how_pandas: str):
        test_data_left = DataFrame.from_records(
            [
                {"col1": "a", "col2": "1"},
                {"col1": "a", "col2": "2"},
                {"col1": "b", "col2": "3"},
                {"col1": "b", "col2": "4"},
                {"col1": "c", "col2": "5"},
                {"col1": "c", "col2": "6"},
            ]
        )

        test_data_right = DataFrame.from_records(
            [
                {"col1": "a", "col3": "foo"},
                {"col1": "b", "col3": "bar"},
                {"col1": "c", "col3": "baz"},
            ]
        )

        with TemporaryDirectory() as workdir:
            workdir = Path(workdir)
            sqlite_file = workdir / "tmp.sqlite"
            with create_sqlite_database(db_file=sqlite_file) as conn:
                table_name_left = "_left"
                table_name_right = "_right"

                table_create(conn, table_name_left, {"col1": "TEXT", "col2": "TEXT"})
                table_create(conn, table_name_right, {"col1": "TEXT", "col3": "TEXT"})
                table_import_from_records(
                    conn, table_name_left, _dataframe_records_iterator(test_data_left)
                )
                table_import_from_records(
                    conn, table_name_right, _dataframe_records_iterator(test_data_right)
                )

                self._check_table_not_empty(conn, table_name_left)
                self._check_table_not_empty(conn, table_name_right)

                expected = test_data_left.merge(test_data_right, on=["col1"], how=how_pandas)

                # Merge and output as an iterable
                result1 = DataFrame.from_records(
                    table_join_sql(
                        conn, table_name_left, table_name_right, on=["col1"], how=how_sqlite
                    )
                )
                self._compare_dataframes_equal(result1, expected)

                # Merge into a table, and output its data
                table_name_merged = "_merged"
                table_join_sql(
                    conn,
                    table_name_left,
                    table_name_right,
                    on=["col1"],
                    how=how_sqlite,
                    into_table=table_name_merged,
                )
                result2 = DataFrame.from_records(table_select_all(conn, table_name_merged))
                self._compare_dataframes_equal(result2, expected)

    def _test_table_merge(self, how_sqlite: str, how_pandas: str):
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

        with TemporaryDirectory() as workdir:
            workdir = Path(workdir)
            sqlite_file = workdir / "tmp.sqlite"
            with create_sqlite_database(db_file=sqlite_file) as conn:
                table_name_1 = "_1"
                table_name_2 = "_2"
                table_name_3 = "_3"

                table_create(conn, table_name_1, {"col1": "TEXT", "col2": "TEXT"})
                table_create(conn, table_name_2, {"col1": "TEXT", "col3": "TEXT"})
                table_create(conn, table_name_3, {"col1": "TEXT", "col4": "TEXT"})
                table_import_from_records(
                    conn, table_name_1, _dataframe_records_iterator(test_data_1)
                )
                table_import_from_records(
                    conn, table_name_2, _dataframe_records_iterator(test_data_2)
                )
                table_import_from_records(
                    conn, table_name_3, _dataframe_records_iterator(test_data_3)
                )

                self._check_table_not_empty(conn, table_name_1)
                self._check_table_not_empty(conn, table_name_2)
                self._check_table_not_empty(conn, table_name_3)

                expected = table_merge_pandas(
                    [test_data_1, test_data_2, test_data_3], on=["col1"], how=how_pandas
                )

                # Merge and output as an iterable
                result1 = DataFrame.from_records(
                    table_merge_sql(
                        conn,
                        [table_name_1, table_name_2, table_name_3],
                        on=["col1"],
                        how=how_sqlite,
                    )
                )
                self._compare_dataframes_equal(result1, expected)

                # Merge into a table, and output its data
                table_name_merged = "_merged"
                table_merge_sql(
                    conn,
                    [table_name_1, table_name_2, table_name_3],
                    on=["col1"],
                    how=how_sqlite,
                    into_table=table_name_merged,
                )
                result2 = DataFrame.from_records(table_select_all(conn, table_name_merged))
                self._compare_dataframes_equal(result2, expected)

    def test_table_join(self):
        self._test_table_join("inner", "inner")
        self._test_table_join("left outer", "left")

    def test_table_merge(self):
        self._test_table_merge("inner", "inner")
        self._test_table_merge("left outer", "left")

    def test_table_records_reimport(self):
        with TemporaryDirectory() as workdir:
            workdir = Path(workdir)

            schema = {_safe_column_name(col): dtype for col, dtype in get_schema().items()}
            sqlite_file = workdir / "tmp.sqlite"
            tables_folder = SRC / "test" / "data"
            with create_sqlite_database(db_file=sqlite_file) as conn:
                for table_path in tables_folder.glob("*.csv"):
                    table_name = _safe_table_name(table_path.stem)
                    table_import_from_file(conn, table_path, schema=schema)

                    # Export the records to a list
                    records_output_1 = list(table_select_all(conn, table_name))

                    # Import the list of records
                    table_name_2 = table_name + "_new"
                    table_import_from_records(conn, table_name_2, records_output_1, schema=schema)

                    # Re-export the records as a list
                    records_output_2 = list(table_select_all(conn, table_name_2))

                    for record1, record2 in zip(records_output_1, records_output_2):
                        self.assertDictEqual(record1, record2)


if __name__ == "__main__":
    sys.exit(main())
