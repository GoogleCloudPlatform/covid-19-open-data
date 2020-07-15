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

import csv
import json
import shutil
import datetime
import cProfile
import traceback
from pstats import Stats
from pathlib import Path
from functools import partial
from argparse import ArgumentParser
from tempfile import TemporaryDirectory
from typing import Dict, Iterable, List, TextIO, Tuple

from pandas import DataFrame, date_range

from lib.concurrent import thread_map
from lib.constants import SRC, EXCLUDE_FROM_MAIN_TABLE
from lib.io import display_progress, read_file, read_lines, export_csv, pbar
from lib.pipeline_tools import get_schema


# Anything under 100 MB can use the fast JSON converter
JSON_FAST_CONVERTER_SIZE_THRESHOLD_BYTES = 100 * 1000 * 1000


def _read_main_table(path: Path) -> DataFrame:
    return read_file(
        path,
        dtype={
            "country_code": "category",
            "country_name": "category",
            "subregion1_code": "category",
            "subregion1_name": "category",
            "subregion2_code": "category",
            "subregion2_name": "category",
            "3166-1-alpha-2": "category",
            "3166-1-alpha-3": "category",
            "aggregation_level": "category",
        },
    )


def _subset_last_days(output_folder: Path, days: int) -> None:
    """ Outputs last N days of data """
    n_days_folder = output_folder / str(days)
    n_days_folder.mkdir(exist_ok=True)
    for csv_file in (output_folder).glob("*.csv"):
        table = read_file(csv_file)

        # Degenerate case: this table has no date
        if not "date" in table.columns or len(table.date.dropna()) == 0:
            export_csv(table, n_days_folder / csv_file.name)
        else:
            last_date = datetime.date.fromisoformat(max(table.date))
            # Since APAC is almost always +1 days ahead, increase the window by 1
            first_date = last_date - datetime.timedelta(days=days + 1)
            export_csv(table[table.date >= first_date.isoformat()], n_days_folder / csv_file.name)


def _subset_latest(output_folder: Path, csv_file: Path) -> None:
    """ Outputs latest data for each key """
    latest_folder = output_folder / "latest"
    latest_folder.mkdir(exist_ok=True)
    output_file = latest_folder / csv_file.name

    with csv_file.open("r") as fd_in:
        reader = csv.reader(fd_in)
        columns = {name: idx for idx, name in enumerate(next(reader))}

        if not "date" in columns.keys():
            # Degenerate case: this table has no date
            shutil.copyfile(csv_file, output_file)
        else:
            has_epi = "total_confirmed" in columns

            # To stay memory-efficient, do the latest subset "by hand" instead of using pandas grouping
            # This assumes that the CSV file is sorted in ascending order, which should always be true
            latest_date: Dict[str, str] = {}
            records: Dict[str, List[str]] = {}
            for record in reader:
                key = record[columns["key"]]
                date = record[columns["date"]]
                total_confirmed = record[columns["total_confirmed"]] if has_epi else True
                latest_seen = latest_date.get(key, date) < date and total_confirmed is not None
                if key not in records or latest_seen:
                    latest_date[key] = date
                    records[key] = record

            with output_file.open("w") as fd_out:
                writer = csv.writer(fd_out)
                writer.writerow(columns.keys())
                for key, record in records.items():
                    writer.writerow(record)


def _subset_grouped_key(main_table_path: Path, output_folder: Path, desc: str = None) -> None:
    """ Outputs a subsets of the table with only records with a particular key """

    # Read the header of the main file to get the columns
    with main_table_path.open("r") as fd:
        header = fd.readline()

    # Do a first sweep to get the number of keys so we can accurately report progress
    key_set = set()
    for line in read_lines(main_table_path):
        key, data = line.split(",", 1)
        key_set.add(key)

    # We make use of the main table being sorted by <key, date> and do a linear sweep of the file
    # assuming that once the key changes we won't see it again in future lines
    current_key: str = None
    file_handle: TextIO = None
    progress_bar = pbar(total=len(key_set), desc=desc)
    for idx, line in enumerate(read_lines(main_table_path)):
        key, data = line.split(",", 1)

        # Skip the header line
        if idx == 0:
            continue

        # When the key changes, close the previous file handle and open a new one
        if current_key != key:
            if file_handle:
                file_handle.close()
            current_key = key
            key_folder = output_folder / key
            key_folder.mkdir(exist_ok=True)
            file_handle = (key_folder / "main.csv").open("w")
            file_handle.write(f"{header}\n")
            progress_bar.update(1)

        file_handle.write(f"{key},{data}")

    # Close the last file handle and we are done
    file_handle.close()
    progress_bar.close()


def convert_csv_to_json_records(
    schema: Dict[str, type], csv_file: Path, json_output: Path = None
) -> None:
    json_coverter_method = convert_csv_to_json_records_fast
    if csv_file.stat().st_size > JSON_FAST_CONVERTER_SIZE_THRESHOLD_BYTES:
        json_coverter_method = convert_csv_to_json_records_slow

    # Converting to JSON is not critical and it may fail in some corner cases
    # As long as the "important" JSON files are created, this should be OK
    try:
        json_coverter_method(schema, csv_file, json_output=json_output)
    except Exception as exc:
        print(f"Unable to convert CSV file {csv_file} to JSON: ${exc}")
        traceback.print_exc()


def convert_csv_to_json_records_slow(
    schema: Dict[str, type], csv_file: Path, json_output: Path = None
) -> None:
    """
    Slow but memory efficient method to convert the provided CSV file to a record-like JSON format
    """
    # JSON output defaults to same as the CSV file but with extension swapped
    if json_output is None:
        json_output = Path(str(csv_file).replace(".csv", ".json"))

    with json_output.open("w") as fd_out:
        # Write the header first
        with csv_file.open("r") as fd_in:
            columns = fd_in.readline().strip().split(",")
            fd_out.write(f'{{"columns":{json.dumps(columns)},"data":[')

        # Read the CSV file in chunks but keep only the values
        first_record = True
        for chunk in read_file(csv_file, chunksize=4096, dtype=schema):
            if first_record:
                first_record = False
            else:
                fd_out.write(",")
            fd_out.write(chunk.to_json(orient="values")[1:-1])

        fd_out.write("]}")


def convert_csv_to_json_records_fast(
    schema: Dict[str, type], csv_file: Path, json_output: Path = None
) -> None:
    """
    Fast but memory intensive method to convert the provided CSV file to a record-like JSON format
    """
    if json_output is None:
        json_output = Path(str(csv_file).replace(".csv", ".json"))

    table = read_file(csv_file, dtype=schema)
    json_dict = json.loads(table.to_json(orient="split"))
    del json_dict["index"]
    with open(json_output, "w") as fd:
        json.dump(json_dict, fd)


def table_cross_product(table1: DataFrame, table2: DataFrame) -> DataFrame:
    tmp_col_name = "_tmp_cross_product_column"
    table1[tmp_col_name] = 1
    table2[tmp_col_name] = 1
    return table1.merge(table2, on=[tmp_col_name]).drop(columns=[tmp_col_name])


def copy_tables(tables_folder: Path, public_folder: Path) -> None:
    """
    Copy tables as-is from the tables folder into the public folder.

    Arguments:
        tables_folder: Input folder where all CSV files exist.
        public_folder: Output folder where the CSV files will be copied to.
    """
    for output_file in pbar([*tables_folder.glob("*.csv")], desc="Copy tables"):
        shutil.copy(output_file, public_folder / output_file.name)


def join_main_tables(tables_folder: Path, output_folder: Path) -> Tuple[Path, Path]:

    # Read the pipeline configs to infer schema
    schema = get_schema()

    # Merge all output files into a single table
    main_by_key = _read_main_table(tables_folder / "index.csv")

    # Add a date to each region from index to allow iterative left joins
    max_date = (datetime.datetime.now() + datetime.timedelta(days=1)).date().isoformat()
    date_list = [date.date().isoformat() for date in date_range("2020-01-01", max_date)]
    date_table = DataFrame(date_list, columns=["date"], dtype="category")
    main_by_key_and_date = table_cross_product(main_by_key[["key"]].copy(), date_table)

    main_by_key.set_index("key", inplace=True)
    main_by_key_and_date.set_index(["key", "date"], inplace=True)

    non_dated_columns = set(main_by_key_and_date.columns)
    for output_file in pbar([*tables_folder.glob("*.csv")], desc="Make main table"):
        table_name = output_file.stem
        if table_name not in EXCLUDE_FROM_MAIN_TABLE:
            # Load the table and perform left outer join
            table = read_file(output_file, low_memory=True, dtype=schema)

            if "date" in table.columns:
                table.set_index(["key", "date"], inplace=True)
                main_by_key_and_date = main_by_key_and_date.join(table)
            else:
                table.set_index(["key"], inplace=True)
                main_by_key = main_by_key.join(table)

                # Keep track of columns which are not indexed by date
                non_dated_columns = non_dated_columns | set(table.columns)

    # Drop rows with null date or without a single dated record
    main_by_key_and_date.reset_index(inplace=True)
    main_by_key_and_date.dropna(how="all", inplace=True)
    main_by_key_and_date.dropna(subset=["date"], inplace=True)

    # Ensure that the tables are appropriately sorted
    main_by_key.reset_index(inplace=True)
    main_by_key.sort_values("key", inplace=True)
    main_by_key_and_date.sort_values(["key", "date"], inplace=True)

    # Write the resulting tables to disk
    main_by_key_path = output_folder / "main-by-key.csv"
    main_by_key_and_date_path = output_folder / "main-by-key-and-date.csv"
    export_csv(main_by_key, main_by_key_path)
    export_csv(main_by_key_and_date, main_by_key_and_date_path)

    return main_by_key_path, main_by_key_and_date_path


def make_main_table(tables_folder: Path, output_path: Path) -> None:
    """
    Build a flat view of all tables combined, joined by <key> or <key, date>.

    Arguments:
        tables_folder: Input folder where all CSV files exist.
    Returns:
        DataFrame: Flat table with all data combined.
    """

    # Merge all output files into a single table
    with TemporaryDirectory() as workdir:
        workdir = Path(workdir)
        main_by_key_path, main_by_key_and_date_path = join_main_tables(tables_folder, workdir)

        # Because the join takes up a lot of memory, we do it by hand... It's much faster this way

        map_by_key = {}
        for line in read_lines(main_by_key_path):
            key, data = line.split(",", 1)
            map_by_key[key] = data.strip()

        with output_path.open("w") as fd:
            for line in read_lines(main_by_key_and_date_path):
                key, data = line.split(",", 1)
                fd.write(f"{key},{data.strip()},{map_by_key[key]}\n")


def create_table_subsets(main_table_path: Path, output_path: Path) -> None:

    # # Create subsets with the last 30, 14 and 7 days of data
    # print("30, 14 and 7 days")
    # map_func = partial(_subset_last_days, output_path)
    # for _ in thread_map(map_func, (30, 14, 7), desc="Last N days subsets"):
    #     pass

    # Create a subset with the latest known day of data for each key
    map_func = partial(_subset_latest, output_path)
    for _ in thread_map(map_func, [*output_path.glob("*.csv")], desc="Latest subset"):
        pass

    # Create subsets with each known key
    _subset_grouped_key(main_table_path, output_path, desc="Grouped key subsets")


def convert_tables_to_json(csv_files: Iterable[Path]):

    # Convert all CSV files to JSON using values format
    map_func = partial(convert_csv_to_json_records, get_schema())
    for _ in thread_map(map_func, csv_files, desc="JSON conversion"):
        pass


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
        main_table = _read_main_table(main_table_path).set_index("key")

        # Create subsets for easy API-like access to slices of data
        create_table_subsets(main_table, v2_folder)

        # Convert all CSV files to JSON using values format
        convert_tables_to_json([*v2_folder.glob("**/*.csv")])


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
