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
import shutil
import warnings
import traceback
from pathlib import Path
from typing import Dict, Iterable, List
from .io import read_lines, read_table


# Any CSV file under 50 MB can use the fast JSON converter
JSON_FAST_CONVERTER_SIZE_BYTES = 50 * 1000 * 1000

# Any CSV file above 150 MB should not be converted to JSON
JSON_MAX_SIZE_BYTES = 150 * 1000 * 1000


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


def table_sort(table_path: Path, output_path: Path) -> None:
    """
    Memory-efficient method used to perform a lexical sort of all the rows of this table, excluding
    the table header.

    Arguments:
        table_path: Path of the table to be sorted.
        output_path: Output location for the sorted table.
    """
    with open(table_path, "r") as fd_in:
        header = next(fd_in)
        with open(output_path, "w") as fd_out:
            fd_out.write(f"{header}")

            records = []
            for record in fd_in:
                records.append(record)

            for record in sorted(records):
                fd_out.write(f"{record}")


def table_join(left: Path, right: Path, on: List[str], output: Path, how: str = "outer") -> None:
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

    def compute_join_indices(columns: List[str]) -> List[str]:
        assert all(
            name in columns.keys() for name in on
        ), f"Column provided in `on` not present in right table. Expected {on} but found {columns}"
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
                if how == "inner" and not key in records_right:
                    continue

                # Get the data from the right table and write to output
                data_right = records_right.get(key, [None] * len(columns_right_output))
                writer.writerow(data_left + data_right)


def skip_head_reader(path: Path, n: int = 1, **read_opts) -> Iterable[str]:
    fd = read_lines(path, **read_opts)
    for _ in range(n):
        next(fd)
    yield from fd


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
    """ Outputs latest data for each key, assumes records are indexed by <key, date> """

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
