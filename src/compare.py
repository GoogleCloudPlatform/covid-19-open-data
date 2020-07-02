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
from pathlib import Path
from argparse import ArgumentParser
from tempfile import TemporaryDirectory
from typing import Set, List

from pandas import DataFrame
from lib.io import read_file
from lib.net import download
from lib.utils import URL_OUTPUTS_PROD
from update import main as update_data


def compare_sets(curr: Set[str], prod: Set[str]) -> List[str]:
    """
    Compares two sets of values and returns a list of items with "+" or "-" prefix indicating
    whether the value has been added or is missing in the second set compared to the first.

    Args:
        curr: Set of new data
        prod: Set of previous data
    Return:
        List[str]: List of differences
    """
    diff_list: List[str] = []
    for val in sorted(curr - prod):
        diff_list += [f"+{val}"]
    for val in sorted(prod - curr):
        diff_list += [f"-{val}"]
    return diff_list


# pylint: disable=redefined-outer-name
def main(output_folder: Path, only: List[str] = None, exclude: List[str] = None):

    # Perform a dry-run to update the data using the current configuration
    update_data(output_folder, only=only, exclude=exclude)

    # Download all the tables from the prod server to local storage
    output_tables = list((output_folder / "tables").glob("*.csv"))
    tables_summary = {table_path.stem: {} for table_path in output_tables}
    for table_path in output_tables:
        table_name = table_path.stem
        table_path_str = str(table_path)
        tables_summary[table_name]["local_curr"] = table_path_str
        local_prod = Path(table_path_str.replace(".csv", ".prod.csv"))
        with local_prod.open(mode="wb") as fd:
            try:
                download(f"{URL_OUTPUTS_PROD}/{table_path.name}", fd)
                tables_summary[table_name]["local_prod"] = str(local_prod)
            except:
                tables_summary[table_name]["local_prod"] = None

    # Compare the new vs prod data
    for table_name, table_data in tables_summary.items():

        # Read both tables into memory
        curr_df = read_file(table_data["local_curr"])
        if table_data["local_prod"] is None:
            prod_df = DataFrame(columns=curr_df.columns)
        else:
            prod_df = read_file(table_data["local_prod"])

        # Compare the number of records
        table_data["records"] = f"{len(curr_df) - len(prod_df):+d}"

        # Compare the columns
        table_data["columns"] = compare_sets(set(curr_df.columns), set(prod_df.columns))

        # Compare the keys
        if "key" in curr_df.columns:
            table_data["keys"] = compare_sets(set(curr_df.key.unique()), set(prod_df.key.unique()))

        # Compare the dates
        if "date" in curr_df.columns:
            table_data["dates"] = compare_sets(
                set(curr_df.date.unique()), set(prod_df.date.unique())
            )

    # Return the summary of changes
    return tables_summary


if __name__ == "__main__":

    # Process command-line arguments
    argparser = ArgumentParser()
    argparser.add_argument("output", type=str)
    argparser.add_argument("--only", type=str, default=None)
    argparser.add_argument("--exclude", type=str, default=None)
    args = argparser.parse_args()

    only = args.only.split(",") if args.only is not None else None
    exclude = args.exclude.split(",") if args.exclude is not None else None

    with TemporaryDirectory() as output_folder:
        output_folder = Path(output_folder)
        tables_summary = main(output_folder, only=only, exclude=exclude)
        with open(args.output, "w") as fd:
            fd.write(json.dumps(tables_summary, indent=4))
