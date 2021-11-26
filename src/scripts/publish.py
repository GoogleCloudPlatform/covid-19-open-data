#!/usr/bin/env python
# Copyright 2021 Google LLC
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
import gzip
import os
import shutil
import sys
from argparse import ArgumentParser
from pathlib import Path
from pstats import Stats
from typing import Dict, Iterable, List, Optional, TextIO

from lib.publish import publish_global_tables
from lib.publish import publish_subset_latest
from lib.publish import publish_location_breakouts
from lib.publish import publish_location_aggregates
from lib.publish import convert_tables_to_json
from lib.publish import merge_location_breakout_tables

# Add our library utils to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from lib.constants import OUTPUT_COLUMN_ADAPTER, SRC, V2_TABLE_LIST, V3_TABLE_LIST
from lib.error_logger import ErrorLogger
from lib.io import pbar, read_lines, temporary_directory
from lib.memory_efficient import table_read_column
from lib.pipeline_tools import get_schema
from lib.time import date_range


def main(output_folder: Path, tables_folder: Path, use_table_names: List[str] = None) -> None:
    """
    This script takes the processed outputs located in `tables_folder` and publishes them into the
    output folder by performing the following operations:

        1. Copy all the tables from `tables_folder` to `output_folder`, renaming fields if
           necessary.
        2. Create different slices of data, such as the latest known record for each region, files
           for the last day of data, files for each individual region.
        3. Produce a main table, created by iteratively performing left outer joins on all other
           tables for each slice of data (bot not for the global tables).
    """
    # Wipe the output folder first
    for item in output_folder.glob("*"):
        if item.name.startswith("."):
            continue
        if item.is_file():
            item.unlink()
        else:
            shutil.rmtree(item)

    # Create the folder which will be published using a stable schema
    output_folder = output_folder / "v3"
    output_folder.mkdir(exist_ok=True, parents=True)

    # Publish the tables containing all location keys
    publish_global_tables(tables_folder, output_folder, V3_TABLE_LIST, OUTPUT_COLUMN_ADAPTER)

    # Publish the latest subset for each table
    latest_folder = output_folder / "latest"
    latest_folder.mkdir(exist_ok=True, parents=True)
    publish_subset_latest(output_folder, latest_folder)

    # Create a temporary folder which will host all the location breakouts
    with temporary_directory() as breakout_folder:

        # Break out each table into separate folders based on the location key
        publish_location_breakouts(output_folder, breakout_folder, use_table_names=use_table_names)

        # Create a folder which will host all the location aggregates
        location_aggregates_folder = output_folder / "location"
        location_aggregates_folder.mkdir(exist_ok=True, parents=True)

        # Aggregate the tables for each location independently
        location_keys = table_read_column(output_folder / "index.csv", "location_key")
        publish_location_aggregates(
            breakout_folder,
            location_aggregates_folder,
            location_keys,
            use_table_names=use_table_names,
        )

    # Create the aggregated table and put it in a compressed file
    agg_file_path = output_folder / "aggregated.csv.gz"
    with gzip.open(agg_file_path, "wt") as compressed_file:
        merge_location_breakout_tables(location_aggregates_folder, compressed_file)

    # Convert all CSV files to JSON using values format
    convert_tables_to_json(output_folder, output_folder)


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

    main(Path(args.output_folder), Path(args.tables_folder), use_table_names=V3_TABLE_LIST)

    if args.profile:
        stats = Stats(profiler)
        stats.strip_dirs()
        stats.sort_stats("cumtime")
        stats.print_stats(20)
