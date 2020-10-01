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

"""
Script used to download a sample of data for testing purposes.
"""

import os
import sys

from pandas import DataFrame
from tqdm import tqdm

# Add our library utils to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# pylint: disable=wrong-import-position
from lib.constants import SRC
from lib.io import export_csv, read_file
from lib.pipeline_tools import get_schema, get_table_names

SAMPLE_KEYS = "AD", "AU_NSW", "CZ", "CZ_10", "US_DE", "US_FL", "US_FL_12001", "US_NY_36061"
COD_URL = "https://storage.googleapis.com/covid19-open-data/v2/{table}.csv"


def fetch_table(table_name: str) -> DataFrame:
    data = read_file(COD_URL.format(table=table_name))
    return data[data["key"].isin(SAMPLE_KEYS)]


def main():
    schema = get_schema()
    for table_name in tqdm(list(get_table_names())):
        table = fetch_table(table_name)
        table = table.sort_values([col for col in ("key", "date") if col in table.columns])
        export_csv(table, path=SRC / "test" / "data" / f"{table_name}.csv", schema=schema)


if __name__ == "__main__":
    main()
