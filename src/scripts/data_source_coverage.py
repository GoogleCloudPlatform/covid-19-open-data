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
from google.cloud.storage import Blob
from pandas import DataFrame
from tqdm import tqdm

path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(path)

from lib.concurrent import process_map, thread_map
from lib.constants import SRC, GCS_BUCKET_TEST
from lib.data_source import DataSource
from lib.io import read_table, temporary_directory
from lib.gcloud import get_storage_bucket
from lib.memory_efficient import get_table_columns, table_read_column
from lib.pipeline import DataPipeline
from lib.pipeline_tools import get_pipelines
from typing import List, Iterable, Dict


def download_file(bucket_name: str, remote_path: str, local_path: str) -> None:
    bucket = get_storage_bucket(bucket_name)
    # print(f"Downloading {remote_path} to {local_path}")
    return bucket.blob(remote_path).download_to_filename(str(local_path))


def read_source_output(data_pipeline: DataPipeline, data_source: DataSource) -> DataFrame:
    with temporary_directory() as workdir:
        output_path = workdir / f"{data_source.uuid(data_pipeline.table)}.csv"
        try:
            download_file(GCS_BUCKET_TEST, f"intermediate/{output_path.name}", output_path)
            columns = get_table_columns(output_path)
            dates = list(table_read_column(output_path, "date")) if "date" in columns else [None]
            return {
                "pipeline": data_pipeline.name,
                "data_source": f"{data_source.__module__}.{data_source.name}",
                "columns": ",".join(columns),
                "first_date": min(dates),
                "last_date": max(dates),
                "location_keys": ",".join(sorted(set(table_read_column(output_path, "key")))),
            }
        except Exception as exc:
            print(exc, file=sys.stderr)
            return []


def get_source_outputs(data_pipelines: Iterable[DataPipeline]) -> Iterable[Dict]:
    """Map a list of pipeline names to their source configs."""

    for data_pipeline in tqdm(list(data_pipelines)):
        # print(f"Processing {data_pipeline.name}")
        map_iter = data_pipeline.data_sources
        map_func = partial(read_source_output, data_pipeline)
        map_opts = dict(desc="Downloading data tables", leave=False)
        yield from thread_map(map_func, map_iter, **map_opts)


if __name__ == "__main__":
    # To authenticate with Cloud locally, run the following commands:
    # > $env:GOOGLE_CLOUD_PROJECT = "github-open-covid-19"
    # > $env:GCS_SERVICE_ACCOUNT = "github-open-covid-19@appspot.gserviceaccount.com"
    # > $env:GCP_TOKEN = $(gcloud auth application-default print-access-token)
    results = DataFrame(get_source_outputs(get_pipelines()))
    results.to_csv(index=False)
