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

# A script to detect all pipelines declared in pipelines/ and cache/ and
# dump a summary of their configurations within a `tmp/` folder in the
# project root.
#
# Example usage: `python src/scripts/list_pipelines.py`

import sys
import os
import json
from functools import partial
from pathlib import Path
from pandas import DataFrame
from tqdm import tqdm

path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(path)

from lib.cast import isna
from lib.concurrent import process_map, thread_map
from lib.constants import SRC, GCS_BUCKET_TEST, GCS_BUCKET_PROD
from lib.data_source import DataSource
from lib.io import pbar, read_table, temporary_directory
from lib.gcloud import download_file
from lib.pipeline import DataPipeline
from lib.pipeline_tools import get_pipelines, enumerate_intermediate_files
from typing import List, Iterable, Dict


TABLES_FOLDER = SRC / ".." / "output" / "tables"
INTERMEDIATE_FOLDER = SRC / ".." / "output" / "intermediate"


def download_intermediate_files(pipeline: DataPipeline) -> None:
    output_folder = INTERMEDIATE_FOLDER
    output_folder.mkdir(parents=True, exist_ok=True)

    for fname in enumerate_intermediate_files(pipeline):
        fpath = output_folder / fname
        if not fpath.exists():
            download_file(GCS_BUCKET_TEST, f"intermediate/{fname}", fpath)


def download_table(pipeline: DataPipeline) -> None:
    output_folder = TABLES_FOLDER
    output_folder.mkdir(parents=True, exist_ok=True)
    fpath = output_folder / f"{pipeline.table}.csv"
    download_file(GCS_BUCKET_PROD, f"v2/{pipeline.table}.csv", fpath)


def trace_data_source(
    pipeline: DataPipeline, intermediate_folder: Path, table_folder: Path
) -> DataFrame:
    combined_table = read_table(table_folder / f"{pipeline.table}.csv", schema=pipeline.schema)

    intermediate_tables = list(pipeline._load_intermediate_results(intermediate_folder))
    # Reverse the order of the files, since priority is higher for later sources
    intermediate_tables = list(reversed(intermediate_tables))

    # Make sure all the tables have the appropriate index
    index_columns = ["key"] + (["date"] if "date" in combined_table.columns else [])
    combined_table.set_index(index_columns, inplace=True)
    for idx, (data_source, table) in enumerate(intermediate_tables):
        # Use grouping since intermediate tables sometimes have duplicate indices
        table = table.groupby(index_columns).last()
        intermediate_tables[idx] = data_source, table

    # Iterate over the indices for each column independently
    source_map: List[Dict[str, str]] = []
    map_opts = dict(total=len(combined_table), desc="Records")
    for idx, record in pbar(combined_table.iterrows(), **map_opts):
        record_sources: Dict[str, str] = {}
        for col in combined_table.columns:
            value = record[col]
            if isna(value):
                # If the record is NaN, data source is NaN too
                # Technically a data source could output NaN values, but we don't care here
                record_sources[col] = None
            else:
                # Otherwise iterate over each intermediate result in order until a match
                # for the value is found
                for data_source, table in intermediate_tables:
                    if col not in table.columns or idx not in table.index:
                        continue
                    if table.loc[idx, col] == value:
                        record_sources[col] = data_source.name
                        break

                # Make sure all data points are accounted for
                assert record_sources.get(col), f"No source found for index {idx} and column {col}"

        source_map.append(record_sources)

    # Create a table with the source map
    source_table = DataFrame(source_map, index=combined_table.index)
    print(source_table)
    return source_table


def main():
    pipeline = DataPipeline.load("hospitalizations")

    print("Downloading combined table")
    download_table(pipeline)
    print("Downloading intermediate files")
    download_intermediate_files(pipeline)
    print("Tracing data sources")
    trace_data_source(pipeline, INTERMEDIATE_FOLDER, TABLES_FOLDER)


if __name__ == "__main__":
    # To authenticate with Cloud locally, run the following commands:
    # > $env:GOOGLE_CLOUD_PROJECT = "github-open-covid-19"
    # > $env:GCS_SERVICE_ACCOUNT = "github-open-covid-19@appspot.gserviceaccount.com"
    # > $env:GCP_TOKEN = $(gcloud auth application-default print-access-token)
    main()
