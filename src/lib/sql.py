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
import json
from pathlib import Path
from sqlite3.dbapi2 import Connection, Cursor, connect
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pandas import Int64Dtype

from .io import open_file_like
from .memory_efficient import line_reader

_SCHEMA_TABLE_NAME = "_table_schemas"
_SCHEMA_TABLE_SCHEMA = {"table_name": "TEXT PRIMARY KEY ON CONFLICT REPLACE", "schema_json": "TEXT"}


def _dtype_to_sql_type(dtype: Any) -> str:
    """
    Parse a dtype name and output the equivalent SQL type.

    Arguments:
        dtype: dtype object.
    Returns:
        str: SQL type name.
    """

    if dtype == "str" or dtype == str:
        return "TEXT"
    if dtype == "float" or dtype == float:
        return "DOUBLE"
    if dtype == "int" or dtype == int or isinstance(dtype, Int64Dtype):
        return "INTEGER"
    raise TypeError(f"Unsupported dtype: {dtype}")


def _safe_column_name(column_name: str) -> str:
    if "." in column_name and column_name[0] != "[":
        column_name = f"[{column_name}]"
    if "-" in column_name:
        column_name = column_name.replace("-", "_")
    if column_name in ("on", "left", "right", "index"):
        column_name = f"_{column_name}"
    if column_name[0].isnumeric():
        column_name = f"_{column_name}"
    return column_name


def _safe_table_name(table_name: str) -> str:
    if "." in table_name:
        table_name = f"[{table_name}]"
    if "-" in table_name:
        table_name = table_name.replace("-", "_")
    if table_name in ("on", "left", "right", "index"):
        table_name = f"_{table_name}"
    return table_name


def _statement_insert_record_tuple(
    conn: Connection,
    table_name: str,
    columns: Tuple[str],
    record: Tuple[str],
    replace: bool = False,
) -> None:
    table_name = _safe_table_name(table_name)
    verb_insert = "INSERT " + ("OR REPLACE " if replace else "")
    placeholders = ", ".join("?" for _ in columns)
    column_names = ", ".join(_safe_column_name(name) for name in columns)
    conn.execute(
        f"{verb_insert} INTO {table_name} ({column_names}) VALUES ({placeholders})", record
    )


def _statement_insert_record_dict(
    conn: Connection, table_name: str, record: Dict[str, str], replace: bool = False
) -> None:
    columns = tuple(record.keys())
    record_values = tuple(record.values())
    _statement_insert_record_tuple(conn, table_name, columns, record_values, replace=replace)


def _fetch_table_schema(conn: Connection, table_name: str) -> Dict[str, str]:
    with conn:
        schema_json = conn.execute(
            f'SELECT schema_json FROM {_SCHEMA_TABLE_NAME} WHERE table_name = "{table_name}"'
        ).fetchone()[0]
        return json.loads(schema_json)


def _output_named_records(cursor: Cursor) -> Iterable[Dict[str, Any]]:
    names = [description[0] for description in cursor.description]
    for record in cursor:
        yield {name: value for name, value in zip(names, record)}
    cursor.close()


def table_create(conn: Connection, table_name: str, schema: Dict[str, str]) -> None:
    table_name = _safe_table_name(table_name)
    sql_schema = ", ".join(f"{_safe_column_name(name)} {dtype}" for name, dtype in schema.items())

    with conn:
        # Create new table (ignore if exists)
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({sql_schema})")

        # Every time a table is created, store its schema in our helper table
        _statement_insert_record_dict(
            conn,
            _SCHEMA_TABLE_NAME,
            {"table_name": table_name, "schema_json": json.dumps(schema)},
            replace=True,
        )


def table_drop(conn: Connection, table_name: str) -> None:
    with conn:
        table_name = _safe_table_name(table_name)
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.execute(f"DELETE FROM {_SCHEMA_TABLE_NAME} WHERE table_name = '{table_name}'")


def create_sqlite_database(db_file: str = None) -> Connection:
    """
    Creates an SQLite database at the specified location, importing all files from the tables
    folder.
    """
    # If no database file is provided, create an in-memory database
    db_file = db_file or ":memory:"
    with connect(db_file) as conn:
        # Create a helper table used to store schemas so we can retrieve them later
        table_create(conn, _SCHEMA_TABLE_NAME, _SCHEMA_TABLE_SCHEMA)
        return conn


def table_import_from_file(
    conn: Connection, table_path: Path, table_name: str = None, schema: Dict[str, str] = None
) -> None:
    """
    Import table from CSV file located at `table_path` using the provided schema for types.

    Arguments:
        cursor: Cursor for the database execution engine
        table_path: Path to the input CSV file
        schema: Pipeline schema for this table
    """
    with conn:

        # Derive table name from file name and open a CSV reader
        table_name = _safe_table_name(table_name or table_path.stem)
        with open_file_like(table_path, mode="r") as fd:
            reader = csv.reader(line_reader(fd, skip_empty=True))

            # Derive header from file
            header = [_safe_column_name(name) for name in next(reader)]
            schema = {_safe_column_name(name): dtype for name, dtype in (schema or {}).items()}

            sql_schema = {}
            for name in header:
                sql_schema[name] = _dtype_to_sql_type(schema.get(name, "str"))

            # Make sure that the table exists and it's empty
            table_drop(conn, table_name)
            table_create(conn, table_name, sql_schema)

            header = sql_schema.keys()
            for record in reader:
                _statement_insert_record_tuple(conn, table_name, header, record)


def table_import_from_records(
    conn: Connection,
    table_name: str,
    records: Iterable[Dict[str, Any]],
    schema: Dict[str, str] = None,
) -> None:
    schema = {_safe_column_name(name): dtype for name, dtype in (schema or {}).items()}

    with conn:
        # Read the first record to derive the header
        if isinstance(records, list):
            first_record = records[0]
            records = records[1:]
        else:
            first_record = next(records)

        # Get the SQL schema from a combination of the record and the provided schema
        sql_schema = {}
        for name in first_record.keys():
            name = _safe_column_name(name)
            sql_schema[name] = _dtype_to_sql_type(schema.get(name, "str"))

        # Create the table in the db and insert the first record
        table_create(conn, table_name, sql_schema)
        _statement_insert_record_dict(conn, table_name, first_record)

        # Insert all remaining records
        for record in records:
            _statement_insert_record_dict(conn, table_name, record)


def table_select_all(
    conn: Connection, table_name: str, sort_by: List[str] = None, sort_ascending: bool = True
) -> Iterable[Dict[str, Any]]:
    table_name = _safe_table_name(table_name)
    statement_select = f"SELECT * FROM {table_name}"
    if sort_by:
        sort_by = ",".join(_safe_column_name(col) for col in sort_by)
        statement_select += f" ORDER BY {sort_by} {'ASC' if sort_ascending else 'DESC'}"
    cursor = conn.execute(statement_select)
    return _output_named_records(cursor)


def table_join(
    conn: Connection,
    left: str,
    right: str,
    on: List[str],
    how: str = "inner",
    into_table: str = None,
) -> Optional[Iterable[Dict[str, Any]]]:
    return table_merge(conn, table_names=[left, right], on=on, how=how, into_table=into_table)


def table_merge(
    conn: Connection,
    table_names: List[str],
    on: List[str],
    how: str = "inner",
    into_table: str = None,
) -> Optional[Iterable[Dict[str, Any]]]:

    table_names = [_safe_table_name(name) for name in table_names]
    assert len(table_names) == len(set(table_names)), f"Table names must all be unique"
    on = [_safe_column_name(col) for col in on]

    left = table_names[0]
    statement_join = f"SELECT * FROM {left}"
    for right in table_names[1:]:
        clause_on = " AND ".join(f"{left}.{col} = {right}.{col}" for col in on)
        statement_join += f" {how.upper()} JOIN {right} ON ({clause_on})"

    with conn:
        if into_table:
            combined_schema = {}
            for table_name in table_names:
                combined_schema.update(_fetch_table_schema(conn, table_name))

            # Make sure the receiving table exists and is empty
            into_table = _safe_table_name(into_table)
            table_drop(conn, into_table)
            table_create(conn, into_table, combined_schema)

            # Coalesce all shared columns
            if len(table_names) > 1:
                coalesce_helper = lambda x: ",".join(f"{table}.{x}" for table in table_names)
                select_map = {col: _safe_column_name(col) for col in combined_schema.keys()}
                select_map.update({col: f"COALESCE({coalesce_helper(col)}) AS {col}" for col in on})
                clause_select = ",".join(select_map[col] for col in combined_schema.keys())
                statement_join = statement_join.replace("SELECT *", f"SELECT {clause_select}")

            # Pre-pend the insert statement so the output goes into the table
            conn.execute(f"INSERT INTO {into_table} " + statement_join)

        # Otherwise perform the merge and yield records from the cursor
        else:
            cursor = conn.execute(statement_join)
            return _output_named_records(cursor)


def table_export_csv(conn: Connection, table_name: str, output_path: Path, **select_opts) -> None:
    with open_file_like(output_path, mode="w") as fd:
        writer = csv.writer(fd)
        records = table_select_all(conn, table_name, **select_opts)
        first_record = next(records)
        writer.writerow(first_record.keys())
        writer.writerow(first_record.values())
        writer.writerows(record.values() for record in records)
