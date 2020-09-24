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
import shutil
import sys
import traceback
import warnings
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, Iterable, List, TextIO

from .io import read_lines, read_table


# Any CSV file under 50 MB can use the fast JSON converter
JSON_FAST_CONVERTER_SIZE_BYTES = 50 * 1000 * 1000

# Any CSV file above 150 MB should not be converted to JSON
JSON_MAX_SIZE_BYTES = 150 * 1000 * 1000


def skip_head_reader(path: Path, n: int = 1, **read_opts) -> Iterable[str]:
    fd = read_lines(path, **read_opts)
    for _ in range(n):
        next(fd)
    yield from fd


def get_table_columns(table_path: Path) -> List[str]:
    """
    Memory-efficient method used to extract the columns of a table without reading the entire
    file into memory.

    Arguments:
        table_path: Path to the table
    Returns:
        List[str]: Column names from the table header
    """
    with open(table_path, "r") as fd:
        reader = csv.reader(fd)
        return next(reader)


def table_sort(table_path: Path, output_path: Path, sort_columns: List[str] = None) -> None:
    """
    Memory-efficient method used to perform a lexical sort of all the rows of this table, excluding
    the table header. This puts the entire table in memory, so it's not exactly memory efficient but
    it's still better than using pandas.

    Arguments:
        table_path: Path of the table to be sorted.
        output_path: Output location for the sorted table.
        sort_columns: Columns used to sort by, defaults to all.
    """
    columns = {name: idx for idx, name in enumerate(get_table_columns(table_path))}
    if not sort_columns:
        sort_columns = [list(columns.keys())[0]]
    assert all(
        col in columns for col in sort_columns
    ), f"Not all columns in input present from list {sort_columns}"
    sort_indices = [columns[name] for name in sort_columns]

    records = []
    reader = csv.reader(skip_head_reader(table_path, skip_empty=True))
    for record in reader:
        records.append(record)

    with open(output_path, "w") as fd_out:
        writer = csv.writer(fd_out)
        writer.writerow(columns.keys())

        for record in sorted(records, key=lambda x: tuple([x[idx] for idx in sort_indices])):
            writer.writerow(record)


def table_join(left: Path, right: Path, on: List[str], output: Path, how: str = "INNER") -> None:
    """
    Performs a memory efficient left join between two CSV files. The records of the right table
    are held in memory, so in case of inner joins where order does not matter it is more efficient
    to pass the bigger table as `left` and smaller one as `right`.

    Arguments:
        left: Left table to join. Only rows present in this table will be present in the output.
        right: Right table to join. All of its columns will be added to those of `left`.
        on: Column names to perform the join.
        output: Path to write the joined table to.
        how: Either "inner" or "outer" indicating whether records present only in the `left` table
            should be dropped or not.
    """
    how = how.upper()
    known_methods = ("INNER", "OUTER")
    assert (
        how in known_methods
    ), f"Unrecognized table join method {how}, it should be one of {known_methods}"

    def compute_join_indices(columns: List[str]) -> List[str]:
        assert all(
            name in columns.keys() for name in on
        ), f"Column {on} not present in right table, found {list(columns.keys())}"
        join_indices = [columns[name] for name in on]
        return join_indices

    records_right = {}
    with open(right, "r") as fd:
        reader = csv.reader(fd)
        columns_right = {name: idx for idx, name in enumerate(next(reader))}
        join_indices = compute_join_indices(columns_right)

        # Only save the data which is not part of the join, which will be added by the left table
        columns_right_output = {
            name: idx for name, idx in columns_right.items() if idx not in join_indices
        }

        for record in reader:
            key = tuple([record[idx] for idx in join_indices])
            data = [record[idx] for idx in columns_right_output.values()]
            records_right[key] = data

    with open(output, "w") as fd_out:
        writer = csv.writer(fd_out)
        with open(left, "r") as fd_in:
            reader = csv.reader(fd_in)
            columns_left = {name: idx for idx, name in enumerate(next(reader))}
            join_indices = compute_join_indices(columns_left)

            # Write the output columns as a header
            columns_output = list(columns_left.keys()) + list(columns_right_output.keys())
            writer.writerow(columns_output)

            for record_left in reader:
                key = tuple([record_left[idx] for idx in join_indices])
                data_left = [record_left[idx] for idx in columns_left.values()]

                # If this is an inner join and the key is not in the right table, drop it
                if how == "INNER" and not key in records_right:
                    continue

                # Get the data from the right table and write to output
                data_right = records_right.get(key, [None] * len(columns_right_output))
                writer.writerow(data_left + data_right)


def table_merge(tables: List[Path], output: Path, on: List[str], how: str = "INNER") -> None:
    """
    Build a flat view of all tables combined, joined by <key> or <key, date>.
    Arguments:
        tables_folder: Input directory where all CSV files exist.
        output_path: Output directory for the resulting main.csv file.
        location_key: Name of the key to use for the location, "key" (v2) or "location_key" (v3)
        include_all: Flag indicating if tables from EXCLUDE_FROM_MAIN_TABLE should be excluded.
    """
    assert len(tables) > 0, f"At least one table required for merging, found {len(tables)}"

    # Early exit: if there's only one table, we only copy it
    if len(tables) == 1:
        shutil.copy(tables[0], output)
        return

    # Early exit: if there are only two tables, it's a simple join
    if len(tables) == 2:
        return table_join(tables[0], tables[1], output=output, on=on, how=how)

    # Use a temporary directory for intermediate files
    with TemporaryDirectory() as workdir:
        workdir = Path(workdir)

        # Use temporary files to avoid computing everything in memory
        temp_input = workdir / "tmp.1.csv"
        temp_output = workdir / "tmp.2.csv"

        # Perform an initial join into the temporary table
        table_join(tables[0], tables[1], output=temp_output, on=on, how=how)
        temp_input, temp_output = temp_output, temp_input

        for table_path in tables[2:-1]:

            # Iteratively perform left outer joins on all tables
            table_join(temp_input, table_path, output=temp_output, on=on, how=how)

            # Flip-flop the temp files to avoid a copy
            temp_input, temp_output = temp_output, temp_input

        # Point the last join into the final output
        table_join(temp_input, tables[-1], output=output, on=on, how=how)


def table_cross_product(left: Path, right: Path, output: Path) -> None:
    """
    Memory efficient method to perform the cross product of all columns in two tables. Columns
    which are present in both tables will be duplicated in the output.

    Arguments:
        left: Left table. All columns from this table will be present in the output.
        right: Right table. All columns from this table will be present in the output.
        output: Path to write the joined table to.
    """
    columns_left = get_table_columns(left)
    columns_right = get_table_columns(right)
    with open(output, "w") as fd:
        writer = csv.writer(fd)
        writer.writerow(columns_left + columns_right)

        reader_left = csv.reader(skip_head_reader(left))
        for record_left in reader_left:
            reader_right = csv.reader(skip_head_reader(right))
            for record_right in reader_right:
                writer.writerow(record_left + record_right)


def table_group_tail(table: Path, output: Path) -> None:
    """ Outputs latest data for each key, assumes records are sorted by <key, date> """

    reader = csv.reader(read_lines(table))
    columns = {name: idx for idx, name in enumerate(next(reader))}

    if not "date" in columns.keys():
        # Degenerate case: this table has no date
        shutil.copyfile(table, output)
    else:
        # To stay memory-efficient, do the latest subset "by hand" instead of using pandas grouping
        # This assumes that the CSV file is sorted in ascending order, which should always be true
        # We simply keep track of records grouped by index and overwrite all values with latest
        records: Dict[str, Dict[str, str]] = {}
        for record in reader:
            if not record:
                continue
            try:
                key = record[columns["key"]]
                if key not in records:
                    records[key] = {name: None for name in columns.keys()}
                for name, idx in columns.items():
                    value = record[idx]
                    if value != "" and value is not None:
                        records[key][name] = value
            except Exception as exc:
                print(f"Error parsing record {record} in table {table}: {exc}", file=sys.stderr)
                traceback.print_exc()

        with open(output, "w") as fd_out:
            writer = csv.writer(fd_out)
            writer.writerow(columns.keys())
            for record in records.values():
                writer.writerow(record[name] for name in columns.keys())


def table_rename(
    table: Path, output: Path, column_adapter: Dict[str, str], drop: bool = False
) -> None:
    """
    Renames the header of the input table leaving column order and all rows the same. If a column
    name is mapped to `None`, then the column will be removed from the output.

    Arguments:
        table: Location of the input table.
        output: Location of the output table.
        column_adapter: Map of <old column name, new column name>.
        drop: Flag indicating whether columns not in the adapter should be dropped
    """
    reader = csv.reader(read_lines(table, skip_empty=True))
    column_indices = {idx: name for idx, name in enumerate(next(reader))}
    output_columns = {
        idx: column_adapter.get(name, None if drop else name)
        for idx, name in column_indices.items()
    }
    output_columns_idx = [idx for idx, name in output_columns.items() if name is not None]

    with open(output, "w") as fd_out:
        writer = csv.writer(fd_out)
        writer.writerow(name for name in output_columns.values() if name is not None)
        for record in reader:
            writer.writerow(record[idx] for idx in output_columns_idx)


def table_filter(table: Path, output: Path, filter_columns: Dict[str, str]) -> None:
    """
    Filter records from table for values which match those specified in `filter_columns`. This
    method can only be used for exact string matches.

    Arguments:
        table: Location of the input table.
        output: Location of the output table.
        filter_columns: Map of key-val pairs where column named <key> must have a value <val>.
    """
    reader = csv.reader(read_lines(table, skip_empty=True))
    columns = {name: idx for idx, name in enumerate(next(reader))}
    filter_values = {columns.get(name): value for name, value in filter_columns.items()}

    with open(output, "w") as fd_out:
        writer = csv.writer(fd_out)
        writer.writerow(columns.keys())
        for record in reader:
            if all(record[idx] == filter_values.get(idx, record[idx]) for idx in columns.values()):
                writer.writerow(record[idx] for idx in columns.values())


def table_breakout(table: Path, output_folder: Path, breakout_column: str) -> None:
    """
    Performs a linear sweep of the input table and breaks it out based on the value of the given
    `breakout_column`. To perform this operation in O(N), the table is expected to be sorted first.

    Arguments:
        table: Location of the input table.
        output_folder: Location of the output directory where the breakout tables will be placed.
        breakout_column: Name of the column to use for the breakout depending on its value.
    """
    reader = csv.reader(read_lines(table, skip_empty=True))
    columns = {name: idx for idx, name in enumerate(next(reader))}
    assert breakout_column in columns, f"Column {breakout_column} not found in table header"
    breakout_column_idx = columns[breakout_column]
    output_header = list(columns.keys())

    # Define variables outside of the loop, which will be modified as we traverse the file
    breakout_folder: Path = None
    csv_writer = None  # type is private
    current_breakout_value: str = None
    file_handle: TextIO = None

    # Keep track of all seen breakout column values
    seen_breakout_values = set()

    # We make use of the main table being sorted by <key, date> and do a linear sweep of the file
    # assuming that once the key changes we won't see it again in future lines
    for record in reader:
        breakout_value = record[breakout_column_idx]

        # When the key changes, close the previous file handle and open a new one
        if current_breakout_value != breakout_value:
            if file_handle:
                file_handle.close()

            if breakout_value in seen_breakout_values:
                raise RuntimeError(f"Table {table} was not sorted by {breakout_column}")
            seen_breakout_values.add(breakout_value)

            current_breakout_value = breakout_value
            breakout_folder = output_folder / breakout_value
            breakout_folder.mkdir(exist_ok=True, parents=True)
            file_handle = (breakout_folder / table.name).open("w")
            csv_writer = csv.writer(file_handle)
            csv_writer.writerow(output_header)

        csv_writer.writerow(record)

    # Close the last file handle and we are done
    if file_handle:
        file_handle.close()


def table_read_column(table: Path, column: str) -> Iterable[str]:
    """
    Read a single column from the input table.

    Arguments:
        table: Location of the input table.
        column: Name of the column to read.
    Returns:
        Iterable[str]: Iterable of values for the requested column
    """
    reader = csv.reader(read_lines(table, skip_empty=True))
    column_indices = {name: idx for idx, name in enumerate(next(reader))}
    output_column_index = column_indices[column]
    for record in reader:
        yield record[output_column_index]


def table_drop_nan_columns(table: Path, output: Path) -> None:
    """
    Drop columns with only null values from the table.

    Arguments:
        table: Location of the input table.
        output: Location of the output table.
    """
    reader = csv.reader(read_lines(table, skip_empty=True))
    column_names = {idx: name for idx, name in enumerate(next(reader))}

    # Perform a linear sweep to look for columns without a single non-null value
    not_nan_columns = set()
    for record in reader:
        for idx, value in enumerate(record):
            if value is not None and value != "":
                not_nan_columns.add(idx)

    # Remove all null columns and write output
    nan_columns = [idx for idx in column_names.keys() if idx not in not_nan_columns]
    table_rename(table, output, {column_names[idx]: None for idx in nan_columns})


def convert_csv_to_json_records(
    schema: Dict[str, type],
    csv_file: Path,
    output_file: Path,
    skip_size_threshold: int = None,
    fast_size_threshold: int = None,
) -> None:

    if skip_size_threshold is None:
        skip_size_threshold = JSON_MAX_SIZE_BYTES
    if fast_size_threshold is None:
        fast_size_threshold = JSON_FAST_CONVERTER_SIZE_BYTES

    file_size = csv_file.stat().st_size
    json_coverter_method = _convert_csv_to_json_records_fast

    if skip_size_threshold > 0 and file_size > skip_size_threshold:
        raise ValueError(f"Size of {csv_file} too large for conversion: {file_size // 1E6} MB")

    if fast_size_threshold > 0 and file_size > fast_size_threshold:
        warnings.warn(f"Size of {csv_file} too large for fast method: {file_size // 1E6} MB")
        json_coverter_method = _convert_csv_to_json_records_slow

    json_coverter_method(schema, csv_file, output_file)


def _convert_csv_to_json_records_slow(schema: Dict[str, type], csv_file: Path, output_file) -> None:
    """
    Slow but memory efficient method to convert the provided CSV file to a record-like JSON format
    """

    with output_file.open("w") as fd_out:
        # Write the header first
        columns = get_table_columns(csv_file)
        columns_str = ",".join([f'"{col}"' for col in columns])
        fd_out.write(f'{{"columns":[{columns_str}],"data":[')

        # Read the CSV file in chunks but keep only the values
        first_record = True
        for chunk in read_table(csv_file, schema=schema, chunksize=256):
            if first_record:
                first_record = False
            else:
                fd_out.write(",")
            fd_out.write(chunk.to_json(orient="values")[1:-1])

        fd_out.write("]}")


def _convert_csv_to_json_records_fast(
    schema: Dict[str, type], csv_file: Path, output_file: Path
) -> None:
    """
    Fast but memory intensive method to convert the provided CSV file to a record-like JSON format
    """
    table = read_table(csv_file, schema=schema)
    json_dict = json.loads(table.to_json(orient="split"))
    del json_dict["index"]
    with open(output_file, "w") as fd:
        json.dump(json_dict, fd)
