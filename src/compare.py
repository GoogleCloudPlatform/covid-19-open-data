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


import sys
import json
from pathlib import Path
from argparse import ArgumentParser
from tempfile import TemporaryDirectory
from typing import Any, Dict, List, Set

from pandas import DataFrame, concat
from lib.io import read_file, display_progress
from lib.net import download
from lib.pipeline import DataPipeline
from lib.utils import URL_OUTPUTS_PROD


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
    for val in sorted(prod - curr):
        diff_list += [f"-{val}"]
    for val in sorted(curr - prod):
        diff_list += [f"+{val}"]
    return diff_list


def compare_tables(table_curr: DataFrame, table_prod: DataFrame) -> Dict[str, Any]:
    cmp_out: Dict[str, Any] = {}

    # Make a copy of the tables to avoid modification
    table_curr = table_curr.copy()
    table_prod = table_prod.copy()

    # Compare the number of records
    cmp_out["records"] = f"{len(table_curr) - len(table_prod):+d}"

    # Compare the columns
    cmp_out["columns"] = compare_sets(set(table_curr.columns), set(table_prod.columns))

    # Create a single, indexable column
    idx_cols = ["key"] + (["date"] if "date" in table_curr.columns else [])
    curr_idx, prod_idx = table_curr[idx_cols[0]], table_prod[idx_cols[0]]
    for col in idx_cols[1:]:
        curr_idx = curr_idx + " " + table_curr[col]
        prod_idx = prod_idx + " " + table_prod[col]

    # Compare the sets of indices
    cmp_out["indices"] = compare_sets(set(curr_idx), set(prod_idx))

    # Compare the shared indices
    table_curr["_cmp_idx"] = curr_idx
    table_prod["_cmp_idx"] = prod_idx
    shared_df = concat([table_curr, table_prod]).drop_duplicates(keep=False)
    cmp_out["modifications"] = shared_df["_cmp_idx"].values.tolist()

    return cmp_out


if __name__ == "__main__":

    # Process command-line arguments
    argparser = ArgumentParser()
    argparser.add_argument("--path", type=str, default=None)
    argparser.add_argument("--table", type=str, default=None)
    argparser.add_argument("--output", type=str, default=None)
    argparser.add_argument("--progress", action="store_true")
    args = argparser.parse_args()

    assert args.path is not None or args.table is not None, "Either --path or --table must be given"

    # Manage whether to show progress with our handy context manager
    with display_progress(args.progress):

        # Derive the name of the table from either provided table name or its path
        table_name = args.table or Path(args.path).stem

        # If we have a path, we can just read the table from there
        if args.path:
            table_curr = read_file(args.path)

        # If there is no local path for the table, we have to produce the table ourselves
        else:
            with TemporaryDirectory() as output_folder:
                output_folder = Path(output_folder)
                pipeline_name = table_name.replace("-", "_")
                data_pipeline = DataPipeline.load(pipeline_name)
                (output_folder / "snapshot").mkdir()
                (output_folder / "intermediate").mkdir()
                table_curr = data_pipeline.run(pipeline_name, output_folder)

        # Download the table from the production server if it exists
        try:
            table_prod = read_file(f"{URL_OUTPUTS_PROD}/{table_name}.csv")
        except:
            table_prod = DataFrame(columns=table_curr.columns)

        output_dict = compare_tables(table_curr, table_prod)
        output_json = json.dumps({"table": table_name, **output_dict}, indent=4)
        if args.output is None:
            sys.stdout.write(output_json)
        else:
            with open(args.output, "w") as fd:
                fd.write(output_json)
