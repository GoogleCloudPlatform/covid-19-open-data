#!/usr/bin/env python
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

import cProfile
import datetime
import shutil
import sys
import traceback
from argparse import ArgumentParser
from functools import partial
from pathlib import Path
from pstats import Stats
from tempfile import TemporaryDirectory
from typing import Dict, Iterable, TextIO

from pandas import DataFrame, date_range

from lib.concurrent import thread_map
from lib.constants import EXCLUDE_FROM_MAIN_TABLE, SRC
from lib.io import display_progress, export_csv, pbar, read_file, read_lines
from lib.memory_efficient import (
    convert_csv_to_json_records,
    get_table_columns,
    table_breakout,
    table_cross_product,
    table_group_tail,
    table_join,
    table_read_column,
    table_rename,
    table_sort,
)
from lib.pipeline_tools import get_schema


def _subset_grouped_key(
    main_table_path: Path, output_folder: Path, desc: str = None
) -> Iterable[Path]:
    """ Outputs a subsets of the table with only records with a particular key """

    # Read the header of the main file to get the columns
    with open(main_table_path, "r") as fd:
        header = next(fd)

    # Do a first sweep to get the number of keys so we can accurately report progress
    key_set = set()
    for line in read_lines(main_table_path, skip_empty=True):
        key, data = line.split(",", 1)
        key_set.add(key)

    # We make use of the main table being sorted by <key, date> and do a linear sweep of the file
    # assuming that once the key changes we won't see it again in future lines
    key_folder: Path = None
    current_key: str = None
    file_handle: TextIO = None
    progress_bar = pbar(total=len(key_set), desc=desc)
    for idx, line in enumerate(read_lines(main_table_path, skip_empty=True)):
        key, data = line.split(",", 1)

        # Skip the header line
        if idx == 0:
            continue

        # When the key changes, close the previous file handle and open a new one
        if current_key != key:
            if file_handle:
                file_handle.close()
            if key_folder:
                yield key_folder / "main.csv"
            current_key = key
            key_folder = output_folder / key
            key_folder.mkdir(exist_ok=True)
            file_handle = (key_folder / "main.csv").open("w")
            file_handle.write(f"{header}")
            progress_bar.update(1)

        file_handle.write(f"{key},{data}")

    # Close the last file handle and we are done
    file_handle.close()
    progress_bar.close()


def copy_tables(tables_folder: Path, public_folder: Path) -> None:
    """
    Copy tables as-is from the tables folder into the public folder.

    Arguments:
        tables_folder: Input folder where all CSV files exist.
        public_folder: Output folder where the CSV files will be copied to.
    """
    for output_file in pbar([*tables_folder.glob("*.csv")], desc="Copy tables"):
        shutil.copy(output_file, public_folder / output_file.name)


def make_main_table(tables_folder: Path, output_path: Path) -> None:
    """
    Build a flat view of all tables combined, joined by <key> or <key, date>.

    Arguments:
        tables_folder: Input folder where all CSV files exist.
    Returns:
        DataFrame: Flat table with all data combined.
    """

    # Use a temporary directory for intermediate files
    with TemporaryDirectory() as workdir:
        workdir = Path(workdir)

        # Merge all output files into a single table
        keys_table_path = workdir / "keys.csv"
        keys_table = read_file(tables_folder / "index.csv", usecols=["key"])
        export_csv(keys_table, keys_table_path)
        print("Created keys table")

        # Add a date to each region from index to allow iterative left joins
        max_date = (datetime.datetime.now() + datetime.timedelta(days=1)).date().isoformat()
        date_list = [date.date().isoformat() for date in date_range("2020-01-01", max_date)]
        date_table_path = workdir / "dates.csv"
        export_csv(DataFrame(date_list, columns=["date"]), date_table_path)
        print("Created dates table")

        # Create a temporary working table file which can be used during the steps
        temp_file_path = workdir / "main.tmp.csv"
        table_cross_product(keys_table_path, date_table_path, temp_file_path)
        print("Created cross product table")

        # Add all the index columns to seed the main table
        main_table_path = workdir / "main.csv"
        table_join(
            temp_file_path, tables_folder / "index.csv", ["key"], main_table_path, how="outer"
        )
        print("Joined with table index")

        non_dated_columns = set(get_table_columns(main_table_path))
        for table_file_path in pbar([*tables_folder.glob("*.csv")], desc="Make main table"):
            table_name = table_file_path.stem
            if table_name not in EXCLUDE_FROM_MAIN_TABLE:

                table_columns = get_table_columns(table_file_path)
                if "date" in table_columns:
                    join_on = ["key", "date"]
                else:
                    join_on = ["key"]

                    # Keep track of columns which are not indexed by date
                    non_dated_columns = non_dated_columns | set(table_columns)

                # Iteratively perform left outer joins on all tables
                table_join(main_table_path, table_file_path, join_on, temp_file_path, how="outer")
                shutil.move(temp_file_path, main_table_path)
                print(f"Joined with table {table_name}")

        # Drop rows with null date or without a single dated record
        # TODO: figure out a memory-efficient way to do this

        # Ensure that the table is appropriately sorted ans write to output location
        table_sort(main_table_path, output_path)
        print("Sorted main table")


def create_table_subsets(main_table_path: Path, output_path: Path) -> Iterable[Path]:

    latest_path = output_path / "latest"
    latest_path.mkdir(parents=True, exist_ok=True)

    def subset_latest(csv_file: Path) -> Path:
        output_file = latest_path / csv_file.name
        table_group_tail(csv_file, output_file)
        return output_file

    # Create a subset with the latest known day of data for each key
    map_func = subset_latest
    yield from thread_map(map_func, [*output_path.glob("*.csv")], desc="Latest subset")

    # Create subsets with each known key
    yield from _subset_grouped_key(main_table_path, output_path, desc="Grouped key subsets")


def convert_tables_to_json(csv_folder: Path, output_folder: Path) -> Iterable[Path]:
    def try_json_covert(schema: Dict[str, str], csv_file: Path) -> Path:
        # JSON output defaults to same as the CSV file but with extension swapped
        json_output = output_folder / str(csv_file.relative_to(csv_folder)).replace(".csv", ".json")
        json_output.parent.mkdir(parents=True, exist_ok=True)

        # Converting to JSON is not critical and it may fail in some corner cases
        # As long as the "important" JSON files are created, this should be OK
        try:
            print(f"Converting {csv_file} to JSON")
            convert_csv_to_json_records(schema, csv_file, json_output)
            return json_output
        except Exception as exc:
            print(f"Unable to convert CSV file {csv_file} to JSON: ${exc}", file=sys.stderr)
            traceback.print_exc()
            return None

    # Convert all CSV files to JSON using values format
    map_iter = list(csv_folder.glob("**/*.csv"))
    map_func = partial(try_json_covert, get_schema())
    for json_output in thread_map(map_func, map_iter, max_workers=2, desc="JSON conversion"):
        if json_output is not None:
            yield json_output


def main(output_folder: Path, tables_folder: Path, show_progress: bool = True) -> None:
    """
    This script takes the processed outputs located in `tables_folder` and publishes them into the
    output folder by performing the following operations:

        1. Copy all the tables as-is from `tables_folder` to `output_folder`
        2. Produce a main table, created by iteratively performing left outer joins on all other
           tables (with a few exceptions)
        3. Create different slices of data, such as the latest known record for each region, files
           for the last N days of data, files for each individual region
    """
    with display_progress(show_progress):

        # Wipe the output folder first
        for item in output_folder.glob("*"):
            if item.name.startswith("."):
                continue
            if item.is_file():
                item.unlink()
            else:
                shutil.rmtree(item)

        # Create the folder which will be published using a stable schema
        v2_folder = output_folder / "v2"
        v2_folder.mkdir(exist_ok=True, parents=True)

        # Copy all output files to the V2 folder
        copy_tables(tables_folder, v2_folder)

        # Create the main table and write it to disk
        main_table_path = v2_folder / "main.csv"
        make_main_table(tables_folder, main_table_path)

        # Create subsets for easy API-like access to slices of data
        create_table_subsets(main_table_path, v2_folder)

        # Convert all CSV files to JSON using values format
        convert_tables_to_json([*v2_folder.glob("**/*.csv")], v2_folder)


if __name__ == "__main__":

    # Process command-line arguments
    output_root = SRC / ".." / "output"
    argparser = ArgumentParser()
    argparser.add_argument("--profile", action="store_true")
    argparser.add_argument("--no-progress", action="store_true")
    argparser.add_argument("--tables-folder", type=str, default=str(output_root / "tables"))
    argparser.add_argument("--output-folder", type=str, default=str(output_root / "public"))
    args = argparser.parse_args()

    if args.profile:
        profiler = cProfile.Profile()
        profiler.enable()

    main(Path(args.output_folder), Path(args.tables_folder), show_progress=not args.no_progress)

    if args.profile:
        stats = Stats(profiler)
        stats.strip_dirs()
        stats.sort_stats("cumtime")
        stats.print_stats(20)
