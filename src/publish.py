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
import traceback
from io import TextIOWrapper
from argparse import ArgumentParser
from functools import partial
from pathlib import Path
from pstats import Stats
from typing import Dict, Iterable, List, TextIO
from zipfile import ZIP_DEFLATED, ZipFile

from lib.constants import OUTPUT_COLUMN_ADAPTER, SRC, V2_TABLE_LIST, V3_TABLE_LIST
from lib.error_logger import ErrorLogger
from lib.io import pbar, read_lines, temporary_directory
from lib.memory_efficient import (
    convert_csv_to_json_records,
    get_table_columns,
    table_breakout,
    table_cross_product,
    table_drop_nan_columns,
    table_grouped_tail,
    table_join,
    table_read_column,
    table_rename,
    table_sort,
)
from lib.pipeline_tools import get_schema
from lib.sql import (
    _safe_table_name,
    create_sqlite_database,
    table_drop as sql_table_drop,
    table_export_csv as sql_export_csv,
    table_import_from_file,
    table_join as sql_table_join,
    table_merge as sql_table_merge,
)
from lib.time import date_range


# Used for debugging purposes
_logger = ErrorLogger("publish")


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


def _get_tables_in_folder(tables_folder: Path, use_table_names: List[str]) -> List[Path]:
    tables_in_folder = {table.stem: table for table in tables_folder.glob("*.csv")}
    tables_found = [tables_in_folder[name] for name in use_table_names if name in tables_in_folder]
    assert tables_found, f"None of the following tables found in {tables_folder}: {use_table_names}"
    return tables_found


def _make_location_key_and_date_table(index_table: Path, output_path: Path) -> None:
    # Use a temporary directory for intermediate files
    with temporary_directory() as workdir:

        # Make sure that there is an index table present
        assert index_table.exists(), "Index table not found"

        # Index table will determine if we use "key" or "location_key" as column name
        index_columns = get_table_columns(index_table)
        location_key = "location_key" if "location_key" in index_columns else "key"

        # Create a single-column table with only the keys
        keys_table_path = workdir / "location_keys.csv"
        with open(keys_table_path, "w") as fd:
            fd.write(f"{location_key}\n")
            fd.writelines(f"{value}\n" for value in table_read_column(index_table, location_key))

        # Add a date to each region from index to allow iterative left joins
        max_date = (datetime.datetime.now() + datetime.timedelta(days=1)).date().isoformat()
        date_table_path = workdir / "dates.csv"
        with open(date_table_path, "w") as fd:
            fd.write("date\n")
            fd.writelines(f"{value}\n" for value in date_range("2020-01-01", max_date))

        # Output all combinations of <key x date>
        table_cross_product(keys_table_path, date_table_path, output_path)


def merge_output_tables(
    tables_folder: Path,
    output_path: Path,
    drop_empty_columns: bool = False,
    use_table_names: List[str] = None,
) -> None:
    """
    Build a flat view of all tables combined, joined by <key> or <key, date>. This function
    requires index.csv to be present under `tables_folder`.

    Arguments:
        tables_folder: Input directory where all CSV files exist.
        output_path: Output directory for the resulting main.csv file.
        drop_empty_columns: Flag determining whether columns with null values only should be
            removed from the output.
        exclude_table_names: Tables which should be removed from the combined output.
    """
    # Default to a known list of tables to use when none is given
    table_paths = _get_tables_in_folder(tables_folder, use_table_names or V2_TABLE_LIST)

    # Use a temporary directory for intermediate files
    with temporary_directory() as workdir:

        # Use temporary files to avoid computing everything in memory
        temp_input = workdir / "tmp.1.csv"
        temp_output = workdir / "tmp.2.csv"

        # Start with all combinations of <location key x date>
        _make_location_key_and_date_table(tables_folder / "index.csv", temp_output)
        temp_input, temp_output = temp_output, temp_input

        for table_file_path in table_paths:
            # Join by <location key> or <location key x date> depending on what's available
            table_columns = get_table_columns(table_file_path)
            join_on = [col for col in ("key", "location_key", "date") if col in table_columns]

            # Iteratively perform left outer joins on all tables
            table_join(temp_input, table_file_path, join_on, temp_output, how="outer")

            # Flip-flop the temp files to avoid a copy
            temp_input, temp_output = temp_output, temp_input

        # Drop rows with null date or without a single dated record
        # TODO: figure out a memory-efficient way to do this

        # Remove columns which provide no data because they are only null values
        if drop_empty_columns:
            table_drop_nan_columns(temp_input, temp_output)
            temp_input, temp_output = temp_output, temp_input

        # Ensure that the table is appropriately sorted and write to output location
        table_sort(temp_input, output_path)


def import_tables_into_sqlite(table_paths: List[Path], output_path: Path) -> None:
    """
    Build a flat view of all tables combined, joined by <key> or <key, date>.

    Arguments:
        table_paths: List of CSV files to join into a single table.
        output_path: Output path for the resulting SQLite file.
    """
    # Import all tables into a database on disk at the provided path
    with create_sqlite_database(output_path) as conn:

        # Get a list of all tables indexed by <location_key> or by <location_key, date>
        schema = get_schema()
        for table_file_path in table_paths:
            table_name = table_file_path.stem
            _logger.log_info(f"Importing {table_name} into SQLite")
            table_columns = get_table_columns(table_file_path)
            table_schema = {col: schema.get(col, str) for col in table_columns}
            table_import_from_file(
                conn, table_file_path, table_name=table_name, schema=table_schema
            )


def merge_output_tables_sqlite(
    tables_folder: Path,
    output_path: Path,
    sqlite_file: Path = None,
    drop_empty_columns: bool = False,
    use_table_names: List[str] = None,
) -> None:
    """
    Build a flat view of all tables combined, joined by <key> or <key, date>. This function
    requires index.csv to be present under `tables_folder`.

    Arguments:
        table_paths: List of CSV files to join into a single table.
        output_path: Output path for the resulting CSV file.
        sqlite_path: Path for the SQLite database to use for importing data, defaults to a temporary
            database on disk.
        drop_empty_columns: Flag determining whether columns with null values only should be
            removed from the output.
    """
    # Default to a known list of tables to use when none is given
    table_paths = _get_tables_in_folder(tables_folder, use_table_names or V2_TABLE_LIST)

    # Use a temporary directory for intermediate files
    with temporary_directory() as workdir:

        # Use two temporary tables as I/O for intermediate operations
        temp_table_input, temp_table_output = "tmp_table_name_1", "tmp_table_name_2"

        # Start with all combinations of <location key x date>
        keys_and_dates_table_path = workdir / f"{temp_table_input}.csv"
        _logger.log_info("Creating keys and dates table")
        index_table = [table for table in table_paths if table.stem == "index"][0]
        _make_location_key_and_date_table(index_table, keys_and_dates_table_path)

        # Create an SQLite database
        _logger.log_info("Importing all tables into SQLite")
        database_file = sqlite_file or workdir / "database.sqlite"
        import_tables_into_sqlite([keys_and_dates_table_path] + table_paths, database_file)

        with create_sqlite_database(database_file) as conn:

            _logger.log_info(f"Merging all tables into a flat output")
            for table in table_paths:
                _logger.log_info(f"Merging {table.stem}")

                # Read the table's header to determine how to merge it
                table_name = _safe_table_name(table.stem)
                table_columns = get_table_columns(table)
                join_on = [col for col in ("key", "location_key", "date") if col in table_columns]

                # Join with the current intermediate table
                sql_table_join(
                    conn,
                    left=temp_table_input,
                    right=table_name,
                    on=join_on,
                    how="left outer",
                    into_table=temp_table_output,
                )

                # Flip-flop the I/O tables to avoid a copy
                temp_table_input, temp_table_output = temp_table_output, temp_table_input

        sort_values = ("location_key", "date")
        _logger.log_info(f"Exporting output as CSV")
        sql_export_csv(conn, temp_table_input, output_path=output_path, sort_by=sort_values)

        # Remove the intermediate tables from the SQLite database
        sql_table_drop(conn, temp_table_input)
        sql_table_drop(conn, temp_table_output)


def _subset_latest(output_folder: Path, csv_file: Path) -> Path:
    output_file = output_folder / csv_file.name
    # Degenerate case: table has no "date" column
    columns = get_table_columns(csv_file)
    if "date" not in columns:
        shutil.copyfile(csv_file, output_file)
    else:
        table_grouped_tail(csv_file, output_file, ["key"])
    return output_file


def create_table_subsets(main_table_path: Path, output_path: Path) -> Iterable[Path]:

    # Create subsets with each known key
    yield from _subset_grouped_key(main_table_path, output_path, desc="Grouped key subsets")

    # Create a subfolder to store the latest row for each key
    latest_path = output_path / "latest"
    latest_path.mkdir(parents=True, exist_ok=True)

    # Create a subset with the latest known day of data for each key
    map_opts = dict(desc="Latest subset")
    map_func = partial(_subset_latest, latest_path)
    map_iter = list(output_path.glob("*.csv")) + [main_table_path]
    yield from pbar(map(map_func, map_iter), **map_opts)


def _try_json_covert(
    schema: Dict[str, str], csv_folder: Path, output_folder: Path, csv_file: Path
) -> Path:
    # JSON output path defaults to same as the CSV file but with extension swapped
    json_output = output_folder / str(csv_file.relative_to(csv_folder)).replace(".csv", ".json")
    json_output.parent.mkdir(parents=True, exist_ok=True)

    # Converting to JSON is not critical and it may fail in some corner cases
    # As long as the "important" JSON files are created, this should be OK
    try:
        _logger.log_debug(f"Converting {csv_file} to {json_output}")
        convert_csv_to_json_records(schema, csv_file, json_output)
        return json_output
    except Exception as exc:
        error_message = f"Unable to convert CSV file {csv_file} to JSON"
        _logger.log_error(error_message, traceback=traceback.format_exc())
        return None


def convert_tables_to_json(csv_folder: Path, output_folder: Path, **tqdm_kwargs) -> Iterable[Path]:

    # Convert all CSV files to JSON using values format
    map_iter = list(csv_folder.glob("**/*.csv"))
    map_opts = dict(total=len(map_iter), desc="Converting to JSON", **tqdm_kwargs)
    map_func = partial(_try_json_covert, get_schema(), csv_folder, output_folder)
    return list(pbar(map(map_func, map_iter), **map_opts))


def publish_location_breakouts(
    tables_folder: Path, output_folder: Path, use_table_names: List[str] = None
) -> List[Path]:
    """
    Breaks out each of the tables in `tables_folder` based on location key, and writes them into
    subdirectories of `output_folder`.

    Arguments:
        tables_folder: Directory containing input CSV files.
        output_folder: Output path for the resulting location data.
    """
    # Default to a known list of tables to use when none is given
    map_iter = _get_tables_in_folder(tables_folder, use_table_names or V2_TABLE_LIST)

    # Break out each table into separate folders based on the location key
    _logger.log_info(f"Breaking out tables {[x.stem for x in map_iter]}")
    map_func = partial(table_breakout, output_folder=output_folder, breakout_column="location_key")
    return list(pbar(map(map_func, map_iter), desc="Breaking out tables"))


def _aggregate_location_breakouts(
    tables_folder: Path, output_folder: Path, key: str, use_table_names: List[str] = None
) -> Path:
    key_output_file = output_folder / key / "main.csv"
    key_output_file.parent.mkdir(parents=True, exist_ok=True)
    merge_output_tables(
        tables_folder / key,
        key_output_file,
        drop_empty_columns=True,
        use_table_names=use_table_names or V2_TABLE_LIST,
    )
    return key_output_file


def publish_location_aggregates(
    tables_folder: Path,
    output_folder: Path,
    location_keys: Iterable[str],
    use_table_names: List[str] = None,
    **tqdm_kwargs,
) -> List[Path]:
    """
    This method joins *all* the tables for each location into a main.csv table.

    Arguments:
        tables_folder: Directory containing input CSV files.
        output_folder: Output path for the resulting location data.
        location_keys: List of location keys to do aggregation for.
    """

    # Create a main.csv file for each of the locations in parallel
    map_iter = list(location_keys)
    _logger.log_info(f"Aggregating outputs for {len(map_iter)} location keys")
    map_opts = dict(total=len(map_iter), desc="Creating location subsets", **tqdm_kwargs)
    map_func = partial(
        _aggregate_location_breakouts, tables_folder, output_folder, use_table_names=use_table_names
    )
    return list(pbar(map(map_func, map_iter), **map_opts))


def publish_global_tables(
    tables_folder: Path, output_folder: Path, use_table_names: List[str] = None
) -> None:
    """
    Copy all the tables from `tables_folder` into `output_folder` converting the column names to the
    latest schema, and join all the tables into a single main.csv file.

    Arguments:
        tables_folder: Input directory containing tables as CSV files.
        output_folder: Directory where the output tables will be written.
    """
    # Default to a known list of tables to use when none is given
    table_paths = _get_tables_in_folder(tables_folder, use_table_names or V2_TABLE_LIST)

    with temporary_directory() as workdir:

        for csv_path in table_paths:
            # Copy all output files to a temporary folder, renaming columns if necessary
            _logger.log_info(f"Renaming columns for {csv_path.name}")
            table_rename(csv_path, workdir / csv_path.name, OUTPUT_COLUMN_ADAPTER)

        for csv_path in table_paths:
            # Sort output files by location key, since the following breakout step requires it
            _logger.log_info(f"Sorting {csv_path.name}")
            table_sort(workdir / csv_path.name, output_folder / csv_path.name, ["location_key"])


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
    v3_folder = output_folder / "v3"
    v3_folder.mkdir(exist_ok=True, parents=True)

    # Publish the tables containing all location keys
    publish_global_tables(tables_folder, v3_folder, use_table_names=use_table_names)

    # Break out each table into separate folders based on the location key
    publish_location_breakouts(v3_folder, v3_folder, use_table_names=use_table_names)

    # Aggregate the independent tables for each location
    location_keys = table_read_column(v3_folder / "index.csv", "location_key")
    publish_location_aggregates(
        v3_folder, v3_folder, location_keys, use_table_names=use_table_names
    )

    # Create a single table aggregating outputs from all other tables
    main_file_name = "covid-19-open-data.csv"
    main_file_zip_path = v3_folder / f"{main_file_name}.zip"
    with ZipFile(main_file_zip_path, mode="w", compression=ZIP_DEFLATED) as zip_archive:
        with zip_archive.open(main_file_name, "w") as output_file:
            merge_output_tables_sqlite(
                v3_folder, TextIOWrapper(output_file), use_table_names=use_table_names
            )

    # Convert all CSV files to JSON using values format
    global_tables = list(v3_folder.glob("*.csv"))
    location_tables = [table for table in v3_folder.glob("**/*.csv") if table not in global_tables]
    convert_tables_to_json(location_tables, v3_folder)


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
