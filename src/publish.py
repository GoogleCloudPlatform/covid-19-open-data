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


import json
import shutil
import datetime
import cProfile
from pstats import Stats
from pathlib import Path
from functools import partial
from argparse import ArgumentParser

from pandas import DataFrame, date_range

from lib.concurrent import thread_map
from lib.io import read_file, export_csv, pbar
from lib.utils import SRC, drop_na_records


def subset_last_days(output_folder: Path, days: int) -> None:
    """ Outputs last N days of data """
    n_days_folder = output_folder / str(days)
    n_days_folder.mkdir(exist_ok=True)
    for csv_file in (output_folder).glob("*.csv"):
        table = read_file(csv_file, low_memory=False)

        # Degenerate case: this table has no date
        if not "date" in table.columns or len(table.date.dropna()) == 0:
            export_csv(table, n_days_folder / csv_file.name)
        else:
            last_date = datetime.date.fromisoformat(max(table.date))
            # Since APAC is almost always +1 days ahead, increase the window by 1
            first_date = last_date - datetime.timedelta(days=days + 1)
            export_csv(table[table.date >= first_date.isoformat()], n_days_folder / csv_file.name)


def subset_latest(output_folder: Path, csv_file: Path) -> DataFrame:
    """ Outputs latest data for each key """
    latest_folder = output_folder / "latest"
    latest_folder.mkdir(exist_ok=True)
    table = read_file(csv_file, low_memory=False)

    # Degenerate case: this table has no date
    if not "date" in table.columns or len(table.date.dropna()) == 0:
        return export_csv(table, latest_folder / csv_file.name)
    else:
        non_null_columns = [col for col in table.columns if not col in ("key", "date")]
        table = table.dropna(subset=non_null_columns, how="all")
        table = table.sort_values("date").groupby(["key"]).tail(1).reset_index()
        export_csv(table, latest_folder / csv_file.name)


def subset_grouped_key(table_indexed: DataFrame, output_folder: Path, key: str) -> None:
    """ Outputs a subset of the table with only records with the given key """
    key_folder = output_folder / key
    key_folder.mkdir(exist_ok=True)
    export_csv(table_indexed.loc[key:key].reset_index(), key_folder / "main.csv")


def export_json_without_index(csv_file: Path) -> None:
    table = read_file(csv_file, low_memory=False)
    json_path = str(csv_file).replace("csv", "json")
    json_dict = json.loads(table.to_json(orient="split"))
    del json_dict["index"]
    with open(json_path, "w") as fd:
        json.dump(json_dict, fd)


def table_cross_product(table1: DataFrame, table2: DataFrame) -> DataFrame:
    tmp_col_name = "_tmp_cross_product_column"
    table1[tmp_col_name] = 1
    table2[tmp_col_name] = 1
    return table1.merge(table2, on=[tmp_col_name]).drop(columns=[tmp_col_name])


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
    # TODO: respect disable progress flag
    disable_progress = not show_progress

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
    for output_file in pbar([*tables_folder.glob("*.csv")], desc="Copy tables"):
        shutil.copy(output_file, v2_folder / output_file.name)

    # Merge all output files into a single table
    main_table = read_file(v2_folder / "index.csv")

    # Add a date to each region from index to allow iterative left joins
    max_date = (datetime.datetime.now() + datetime.timedelta(days=7)).date().isoformat()
    date_list = [date.date().isoformat() for date in date_range("2020-01-01", max_date)]
    date_table = DataFrame(date_list, columns=["date"], dtype=str)
    main_table = table_cross_product(main_table, date_table)

    # Some tables are not included into the main table
    exclude_from_main_table = (
        "main.csv",
        "index.csv",
        "worldbank.csv",
        "worldpop.csv",
        "by-age.csv",
        "by-sex.csv",
    )

    non_dated_columns = set(main_table.columns)
    for output_file in pbar([*v2_folder.glob("*.csv")], desc="Main table"):
        if output_file.name not in exclude_from_main_table:
            # Load the table and perform left outer join
            table = read_file(output_file, low_memory=False)
            main_table = main_table.merge(table, how="left")
            # Keep track of columns which are not indexed by date
            if not "date" in table.columns:
                non_dated_columns = non_dated_columns | set(table.columns)

    # There can only be one record per <key, date> pair
    main_table = main_table.groupby(["key", "date"]).first().reset_index()

    # Drop rows with null date or without a single dated record
    main_table = drop_na_records(main_table.dropna(subset=["date"]), non_dated_columns)
    export_csv(main_table, v2_folder / "main.csv")

    # Create subsets with the last 30, 14 and 7 days of data
    map_func = partial(subset_last_days, v2_folder)
    for _ in thread_map(map_func, (30, 14, 7), desc="Last N days subsets"):
        pass

    # Create a subset with the latest known day of data for each key
    map_func = partial(subset_latest, v2_folder)
    for _ in thread_map(map_func, [*(v2_folder).glob("*.csv")], desc="Latest subset"):
        pass

    # Create subsets with each known key
    main_indexed = main_table.set_index("key")
    map_func = partial(subset_grouped_key, main_indexed, v2_folder)
    for _ in thread_map(map_func, main_indexed.index.unique(), desc="Grouped key subsets"):
        pass

    # Convert all CSV files to JSON using values format
    map_func = export_json_without_index
    for _ in thread_map(map_func, [*(v2_folder).glob("**/*.csv")], desc="JSON conversion"):
        pass


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
