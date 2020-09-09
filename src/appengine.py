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

import datetime
import json
import os.path
import re
import sys
import time
import traceback
from argparse import ArgumentParser
from functools import partial
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Callable, Dict, List

from flask import Flask, request
from google.cloud import storage
from google.cloud.storage.blob import Blob
from google.oauth2.credentials import Credentials

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# pylint: disable=wrong-import-position
from publish import copy_tables, convert_tables_to_json, create_table_subsets, make_main_table
from scripts.cloud_error_processing import register_new_errors

from lib.concurrent import thread_map
from lib.constants import GCS_BUCKET_PROD, GCS_BUCKET_TEST, SRC
from lib.io import export_csv
from lib.net import download
from lib.pipeline import DataPipeline
from lib.pipeline_tools import get_table_names

app = Flask(__name__)
BLOB_OP_MAX_RETRIES = 10
ENV_TOKEN = "GCP_TOKEN"
ENV_PROJECT = "GOOGLE_CLOUD_PROJECT"
ENV_BUCKET = "GCS_BUCKET_NAME"


def get_storage_client():
    """
    Creates an instance of google.cloud.storage.Client using a token if provided via, otherwise
    the default credentials are used.
    """
    gcp_token = os.getenv(ENV_TOKEN)
    gcp_project = os.getenv(ENV_PROJECT)

    client_opts = {}
    if gcp_token is not None:
        client_opts["credentials"] = Credentials(gcp_token)
    if gcp_project is not None:
        client_opts["project"] = gcp_project

    return storage.Client(**client_opts)


def get_storage_bucket(bucket_name: str):
    """
    Gets an instance of the storage bucket for the specified bucket name
    """
    client = get_storage_client()

    # If bucket name is not provided, read it from env var
    bucket_name = bucket_name or os.getenv(ENV_BUCKET)
    assert bucket_name is not None, f"{ENV_BUCKET} not set"
    return client.bucket(bucket_name)


def download_folder(
    bucket_name: str,
    remote_path: str,
    local_folder: Path,
    filter_func: Callable[[Path], bool] = None,
) -> None:
    bucket = get_storage_bucket(bucket_name)

    def _download_blob(local_folder: Path, blob: Blob) -> None:
        # Remove the prefix from the remote path
        rel_path = blob.name.split(f"{remote_path}/", 1)[-1]
        if filter_func is None or filter_func(Path(rel_path)):
            print(f"Downloading {rel_path} to {local_folder}/")
            file_path = local_folder / rel_path
            file_path.parent.mkdir(parents=True, exist_ok=True)
            for i in range(BLOB_OP_MAX_RETRIES):
                try:
                    return blob.download_to_filename(str(file_path))
                except Exception as exc:
                    traceback.print_exc()
                    # Exponential back-off
                    time.sleep(2 ** i)
            raise IOError(f"Error downloading {rel_path}")

    map_func = partial(_download_blob, local_folder)
    _ = thread_map(map_func, bucket.list_blobs(prefix=remote_path), total=None, disable=True)
    list(_)  # consume the results


def upload_folder(
    bucket_name: str,
    remote_path: str,
    local_folder: Path,
    filter_func: Callable[[Path], bool] = None,
) -> None:
    bucket = get_storage_bucket(bucket_name)

    def _upload_file(remote_path: str, file_path: Path):
        target_path = file_path.relative_to(local_folder)
        if filter_func is None or filter_func(target_path):
            print(f"Uploading {target_path} to {remote_path}/")
            blob = bucket.blob(os.path.join(remote_path, target_path))
            for i in range(BLOB_OP_MAX_RETRIES):
                try:
                    return blob.upload_from_filename(str(file_path))
                except Exception as exc:
                    traceback.print_exc()
                    # Exponential back-off
                    time.sleep(2 ** i)
            raise IOError(f"Error uploading {target_path}")

    map_func = partial(_upload_file, remote_path)
    _ = thread_map(map_func, local_folder.glob("**/*.*"), total=None, disable=True)
    list(_)  # consume the results


def cache_build_map() -> Dict[str, List[str]]:
    sitemap: Dict[str, List[str]] = {}
    bucket = get_storage_bucket(GCS_BUCKET_TEST)
    for blob in bucket.list_blobs(prefix="cache"):
        filename = blob.name.split("/")[-1]
        if filename == "sitemap.json":
            continue
        sitemap_key = filename.split(".")[0]
        sitemap[sitemap_key] = sitemap.get(sitemap_key, [])
        sitemap[sitemap_key].append(blob.name)

    # Sort all the cache items
    for sitemap_key, snapshot_list in sitemap.items():
        sitemap[sitemap_key] = list(sorted(snapshot_list))

    return sitemap


@app.route("/cache_pull")
def cache_pull() -> None:
    with TemporaryDirectory() as workdir:
        workdir = Path(workdir)
        now = datetime.datetime.utcnow()
        output_folder = workdir / now.strftime("%Y-%m-%d-%H")
        output_folder.mkdir(parents=True, exist_ok=True)

        def _pull_source(cache_source: Dict[str, str]):
            url = cache_source.pop("url")
            output = cache_source.pop("output")
            buffer = BytesIO()
            try:
                download(url, buffer)
                with (output_folder / output).open("wb") as fd:
                    fd.write(buffer.getvalue())
            except:
                print(f"Cache pull failed for {url}")
                traceback.print_exc()

        # Pull each of the sources from the cache config
        with (SRC / "cache" / "config.json").open("r") as fd:
            cache_list = json.load(fd)
        list(thread_map(_pull_source, cache_list))

        # Upload all cached data to the bucket
        upload_folder(GCS_BUCKET_PROD, "cache", workdir)

        # Build the sitemap for all cached files
        print("Building sitemap")
        sitemap = cache_build_map()
        bucket = get_storage_bucket(GCS_BUCKET_PROD)
        blob = bucket.blob("cache/sitemap.json")
        blob.upload_from_string(json.dumps(sitemap))

    return "OK"


@app.route("/update_table")
def update_table(table_name: str = None, job_group: int = None) -> str:
    table_name = table_name or request.args.get("table")
    assert table_name in list(get_table_names())
    try:
        job_group = request.args.get("job_group")
    except:
        pass
    with TemporaryDirectory() as output_folder:
        output_folder = Path(output_folder)
        (output_folder / "snapshot").mkdir(parents=True, exist_ok=True)
        (output_folder / "intermediate").mkdir(parents=True, exist_ok=True)

        # Load the pipeline configuration given its name
        pipeline_name = table_name.replace("-", "_")
        data_pipeline = DataPipeline.load(pipeline_name)

        # Limit the sources to only the job_group provided
        if job_group is not None:
            data_pipeline.data_sources = [
                data_source
                for data_source in data_pipeline.data_sources
                if data_source.config.get("automation", {}).get("job_group") == job_group
            ]
            assert (
                data_pipeline.data_sources
            ), f"No data sources matched job group {job_group} for table {table_name}"

        # Log the data sources being extracted
        data_source_names = [src.config.get("name") for src in data_pipeline.data_sources]
        print(f"Data sources: {data_source_names}")

        # Produce the intermediate files from the data source
        intermediate_results = data_pipeline.parse(output_folder, process_count=1)
        data_pipeline._save_intermediate_results(
            output_folder / "intermediate", intermediate_results
        )

        # Upload results to the test bucket because these are not prod files
        upload_folder(GCS_BUCKET_TEST, "snapshot", output_folder / "snapshot")
        upload_folder(GCS_BUCKET_TEST, "intermediate", output_folder / "intermediate")

    return "OK"


@app.route("/combine_table")
def combine_table(table_name: str = None) -> str:
    try:
        table_name = request.args.get("table")
    except:
        pass
    assert table_name in list(get_table_names())
    with TemporaryDirectory() as output_folder:
        output_folder = Path(output_folder)
        (output_folder / "tables").mkdir(parents=True, exist_ok=True)

        # Load the pipeline configuration given its name
        pipeline_name = table_name.replace("-", "_")
        data_pipeline = DataPipeline.load(pipeline_name)

        # Get a list of the intermediate files used by this data pipeline
        intermediate_file_names = []
        for data_source in data_pipeline.data_sources:
            intermediate_file_names.append(f"{data_source.uuid(data_pipeline.table)}.csv")

        # Download only the necessary intermediate files
        download_folder(
            GCS_BUCKET_TEST,
            "intermediate",
            output_folder / "intermediate",
            lambda x: x.name in intermediate_file_names,
        )

        # Re-load all intermediate results
        intermediate_results = data_pipeline._load_intermediate_results(
            output_folder / "intermediate"
        )

        # Combine all intermediate results into a single dataframe
        pipeline_output = data_pipeline.combine(intermediate_results)

        # Output combined data to disk
        export_csv(
            pipeline_output,
            output_folder / "tables" / f"{table_name}.csv",
            schema=data_pipeline.schema,
        )

        # Upload results to the test bucket because these are not prod files
        # They will be copied to prod in the publish step, so main.csv is in sync
        upload_folder(GCS_BUCKET_TEST, "tables", output_folder / "tables")

    return "OK"


@app.route("/publish")
def publish() -> str:
    with TemporaryDirectory() as workdir:
        workdir = Path(workdir)
        tables_folder = workdir / "tables"
        public_folder = workdir / "public"
        tables_folder.mkdir(parents=True, exist_ok=True)
        public_folder.mkdir(parents=True, exist_ok=True)

        # Download all the combined tables into our local storage
        download_folder(GCS_BUCKET_TEST, "tables", tables_folder)

        # Prepare all files for publishing and add them to the public folder
        copy_tables(tables_folder, public_folder)
        print("Output tables copied to public folder")

        # Create the joint main table for all records
        main_table_path = public_folder / "main.csv"
        make_main_table(tables_folder, main_table_path)
        print("Main table created")

        # Create subsets for easy API-like access to slices of data
        list(create_table_subsets(main_table_path, public_folder))
        print("Table subsets created")

        # Upload the results to the prod bucket
        upload_folder(GCS_BUCKET_PROD, "v2", public_folder)

    return "OK"


def _convert_json(expr: str) -> str:
    with TemporaryDirectory() as workdir:
        workdir = Path(workdir)
        json_folder = workdir / "json"
        public_folder = workdir / "public"
        json_folder.mkdir(parents=True, exist_ok=True)
        public_folder.mkdir(parents=True, exist_ok=True)

        # Download all the processed tables into our local storage
        download_folder(GCS_BUCKET_PROD, "v2", public_folder, lambda x: re.match(expr, str(x)))

        # Convert all files to JSON
        list(convert_tables_to_json(public_folder, json_folder))
        print("CSV files converted to JSON")

        # Upload the results to the prod bucket
        upload_folder(GCS_BUCKET_PROD, "v2", json_folder)

    return "OK"


@app.route("/convert_json_1")
def convert_json_1() -> str:
    return _convert_json(r"(latest\/)?[a-z_]+.csv")


@app.route("/convert_json_2")
def convert_json_2() -> str:
    return _convert_json(r"[A-Z]{2}(_\w+)?\/\w+.csv")


@app.route("/report_errors_to_github")
def report_errors_to_github() -> str:
    register_new_errors(os.getenv(ENV_PROJECT))
    return "OK"


def main() -> None:
    # Process command-line arguments
    argparser = ArgumentParser()
    argparser.add_argument("--command", type=str, default=None)
    argparser.add_argument("--args", type=str, default=None)
    argparser.add_argument("--debug", action="store_true")
    args = argparser.parse_args()

    def _start_server():
        if args.debug:
            # To authenticate with Cloud locally, run the following commands:
            # > $env:GOOGLE_CLOUD_PROJECT = "github-open-covid-19"
            # > $env:GCP_TOKEN = $(gcloud auth application-default print-access-token)
            app.run(host="127.0.0.1", port=8080, debug=True)
        else:
            app.run(host="0.0.0.0", port=80, debug=False)

    def _unknown_command(*func_args):
        print(f"Unknown command {args.command}", file=sys.stderr)

    # If a command + args are supplied, call the corresponding function
    {
        "server": _start_server,
        "update_table": update_table,
        "combine_table": combine_table,
        "cache_pull": cache_pull,
        "publish": publish,
        "convert_json": _convert_json,
        "report_errors_to_github": report_errors_to_github,
    }.get(args.command, _unknown_command)(**json.loads(args.args or "{}"))


if __name__ == "__main__":
    main()
