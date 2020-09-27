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
from typing import Callable, Dict, List, Optional

import requests
import yaml
from flask import Flask, Response, request
from google.cloud import storage
from google.cloud.storage.blob import Blob
from google.oauth2.credentials import Credentials

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# pylint: disable=wrong-import-position
from publish import copy_tables, convert_tables_to_json, create_table_subsets, make_main_table
from scripts.cloud_error_processing import register_new_errors

from lib.concurrent import thread_map
from lib.constants import GCS_BUCKET_PROD, GCS_BUCKET_TEST, SRC
from lib.error_logger import ErrorLogger
from lib.gcloud import delete_instance, get_internal_ip, start_instance
from lib.io import export_csv
from lib.net import download
from lib.pipeline import DataPipeline
from lib.pipeline_tools import get_table_names

app = Flask(__name__)
logger = ErrorLogger("appengine")

BLOB_OP_MAX_RETRIES = 10
ENV_TOKEN = "GCP_TOKEN"
ENV_PROJECT = "GOOGLE_CLOUD_PROJECT"
ENV_SERVICE_ACCOUNT = "GCS_SERVICE_ACCOUNT"


def _get_request_param(name: str, default: str = None) -> Optional[str]:
    try:
        return request.args.get(name)
    except:
        return default


def get_storage_client() -> storage.Client:
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


def get_storage_bucket(bucket_name: str) -> storage.Bucket:
    """
    Gets an instance of the storage bucket for the specified bucket name
    """
    client = get_storage_client()
    assert bucket_name is not None
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
            logger.log_debug(f"Downloading {rel_path} to {local_folder}/")
            file_path = local_folder / rel_path
            file_path.parent.mkdir(parents=True, exist_ok=True)
            for i in range(BLOB_OP_MAX_RETRIES):
                try:
                    return blob.download_to_filename(str(file_path))
                except Exception as exc:
                    log_message = f"Error downloading {rel_path}."
                    logger.log_warning(log_message, traceback=traceback.format_exc())
                    # Exponential back-off
                    time.sleep(2 ** i)

            # If error persists, there must be something wrong with the network so we are better
            # off crashing the appengine server.
            error_message = f"Error downloading {rel_path}"
            logger.log_error(error_message)
            raise IOError(error_message)

    map_func = partial(_download_blob, local_folder)
    map_iter = bucket.list_blobs(prefix=remote_path)
    list(thread_map(map_func, map_iter, total=None, disable=True, max_workers=8))


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
            logger.log_debug(f"Uploading {target_path} to {remote_path}/")
            blob = bucket.blob(os.path.join(remote_path, target_path))
            for i in range(BLOB_OP_MAX_RETRIES):
                try:
                    return blob.upload_from_filename(str(file_path))
                except Exception as exc:
                    log_message = f"Error uploading {target_path}."
                    logger.log_warning(log_message, traceback=traceback.format_exc())
                    # Exponential back-off
                    time.sleep(2 ** i)

            # If error persists, there must be something wrong with the network so we are better
            # off crashing the appengine server.
            error_message = f"Error uploading {target_path}"
            logger.log_error(error_message)
            raise IOError(error_message)

    map_func = partial(_upload_file, remote_path)
    map_iter = local_folder.glob("**/*.*")
    list(thread_map(map_func, map_iter, total=None, disable=True, max_workers=8))


def cache_build_map() -> Dict[str, List[str]]:
    sitemap: Dict[str, List[str]] = {}
    bucket = get_storage_bucket(GCS_BUCKET_PROD)
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
def cache_pull() -> Response:
    with TemporaryDirectory() as workdir:
        workdir = Path(workdir)
        now = datetime.datetime.utcnow()
        output_folder = workdir / now.strftime("%Y-%m-%d-%H")
        output_folder.mkdir(parents=True, exist_ok=True)

        def _pull_source(cache_source: Dict[str, str]):
            url = cache_source.pop("url")
            output = cache_source.pop("output")
            logger.log_info(f"Downloading {url} into {output}")
            buffer = BytesIO()
            try:
                download(url, buffer)
                with (output_folder / output).open("wb") as fd:
                    fd.write(buffer.getvalue())
                logger.log_info(f"Downloaded {output} successfully")
            except:
                logger.log_error(f"Cache pull failed for {url}.", traceback=traceback.format_exc())

        # Pull each of the sources from the cache config
        with (SRC / "cache.yaml").open("r") as fd:
            cache_list = yaml.safe_load(fd)
        list(thread_map(_pull_source, cache_list, disable=True))

        # Upload all cached data to the bucket
        upload_folder(GCS_BUCKET_PROD, "cache", workdir)

        # Build the sitemap for all cached files
        logger.log_info("Building sitemap")
        sitemap = cache_build_map()
        bucket = get_storage_bucket(GCS_BUCKET_PROD)
        blob = bucket.blob("cache/sitemap.json")
        blob.upload_from_string(json.dumps(sitemap))

    return Response("OK", status=200)


@app.route("/update_table")
def update_table(table_name: str = None, job_group: int = None) -> Response:
    table_name = _get_request_param("table", table_name)
    job_group = _get_request_param("job_group", job_group)

    # Early exit: table name not found
    if table_name not in list(get_table_names()):
        return Response(f"Invalid table name {table_name}", status=400)

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

            # Early exit: job group contains no data sources
            if not data_pipeline.data_sources:
                return Response(
                    f"No data sources matched job group {job_group} for table {table_name}",
                    status=400,
                )

        # Log the data sources being extracted
        data_source_names = [src.config.get("name") for src in data_pipeline.data_sources]
        logger.log_info(f"Data sources: {data_source_names}")

        # Produce the intermediate files from the data source
        intermediate_results = data_pipeline.parse(output_folder, process_count=1)
        data_pipeline._save_intermediate_results(
            output_folder / "intermediate", intermediate_results
        )

        # Upload results to the test bucket because these are not prod files
        upload_folder(GCS_BUCKET_TEST, "snapshot", output_folder / "snapshot")
        upload_folder(GCS_BUCKET_TEST, "intermediate", output_folder / "intermediate")

    return Response("OK", status=200)


@app.route("/combine_table")
def combine_table(table_name: str = None) -> Response:
    table_name = _get_request_param("table", table_name)

    # Early exit: table name not found
    if table_name not in list(get_table_names()):
        return Response(f"Invalid table name {table_name}", status=400)

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

    return Response("OK", status=200)


@app.route("/publish_tables")
def publish_tables() -> Response:
    with TemporaryDirectory() as workdir:
        workdir = Path(workdir)
        input_folder = workdir / "input"
        output_folder = workdir / "output"
        input_folder.mkdir(parents=True, exist_ok=True)
        output_folder.mkdir(parents=True, exist_ok=True)

        # Download all the combined tables into our local storage
        download_folder(GCS_BUCKET_TEST, "tables", input_folder)

        # TODO: perform some validation on the outputs and report errors
        # See: https://github.com/GoogleCloudPlatform/covid-19-open-data/issues/186

        # Prepare all files for publishing and add them to the public folder
        copy_tables(input_folder, output_folder)
        logger.log_info("Output tables copied to public folder")

        # Upload the results to the prod bucket
        upload_folder(GCS_BUCKET_PROD, "v2", output_folder)

    return Response("OK", status=200)


@app.route("/publish_main_table")
def publish_main_table() -> Response:
    with TemporaryDirectory() as workdir:
        workdir = Path(workdir)
        input_folder = workdir / "input"
        output_folder = workdir / "output"
        input_folder.mkdir(parents=True, exist_ok=True)
        output_folder.mkdir(parents=True, exist_ok=True)

        # Download the already published tables
        allowlist_filenames = [f"{table}.csv" for table in get_table_names()]
        download_folder(
            GCS_BUCKET_PROD, "v2", input_folder, lambda x: str(x) in allowlist_filenames
        )

        # Create the joint main table for all records
        main_table_path = output_folder / "main.csv"
        make_main_table(input_folder, main_table_path)
        logger.log_info("Main table created")

        # Upload the results to the prod bucket
        upload_folder(GCS_BUCKET_PROD, "v2", output_folder)

    return Response("OK", status=200)


@app.route("/publish_subset_tables")
def publish_subset_tables() -> Response:
    with TemporaryDirectory() as workdir:
        workdir = Path(workdir)
        input_folder = workdir / "input"
        output_folder = workdir / "output"
        input_folder.mkdir(parents=True, exist_ok=True)
        output_folder.mkdir(parents=True, exist_ok=True)

        # Download the main table only
        download_folder(GCS_BUCKET_PROD, "v2", input_folder, lambda x: str(x) == "main.csv")

        # Create subsets for easy API-like access to slices of data
        main_table_path = input_folder / "main.csv"
        list(create_table_subsets(main_table_path, output_folder))
        logger.log_info("Table subsets created")

        # Upload the results to the prod bucket
        upload_folder(GCS_BUCKET_PROD, "v2", output_folder)

    return Response("OK", status=200)


def _convert_json(expr: str = r"*.csv") -> Response:
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
        logger.log_info("CSV files converted to JSON")

        # Upload the results to the prod bucket
        upload_folder(GCS_BUCKET_PROD, "v2", json_folder)

    return Response("OK", status=200)


@app.route("/convert_json_1")
def convert_json_1() -> Response:
    return _convert_json(r"(latest\/)?[a-z_]+.csv")


@app.route("/convert_json_2")
def convert_json_2() -> Response:
    return _convert_json(r"[A-Z]{2}(_\w+)?\/\w+.csv")


@app.route("/report_errors_to_github")
def report_errors_to_github() -> Response:
    register_new_errors(os.getenv(ENV_PROJECT))
    return Response("OK", status=200)


@app.route("/deferred/<path:url_path>")
def deferred_route(url_path: str) -> Response:
    status = 500
    content = "Unknown error"

    # Initialize the instance ID variable outside of the try so it's available at finally
    instance_id = None

    # Prevent chaining deferred calls
    if any([token == "deferred" for token in url_path.split("/")]):
        return Response(status=400)

    try:
        # Create a new preemptible instance and wait for it to come online
        instance_id = start_instance(service_account=os.getenv(ENV_SERVICE_ACCOUNT))
        instance_ip = get_internal_ip(instance_id)
        logger.log_info(f"Created worker instance {instance_id} with internal IP {instance_ip}")

        # Wait 4 minutes before attempting to forward the request
        log_interval = 30
        wait_seconds = 4 * 60
        for _ in range(wait_seconds // log_interval):
            logger.log_info("Waiting for instance to start...")
            time.sleep(log_interval)

        # Forward the route to the worker instance
        url_fwd = f"http://{instance_ip}/{url_path}"
        logger.log_info(f"Forwarding request to {url_fwd}")
        params = dict(request.args)
        headers = dict(request.headers)
        response = requests.get(url=url_fwd, headers=headers, params=params, timeout=60 * 60 * 2)

        # Retrieve the response from the worker instance
        status = response.status_code
        content = response.content
        logger.log_info(f"Received response with status code {status}")

    except:
        traceback.print_exc()
        content = "Internal exception"

    finally:
        # Shut down the worker instance now that the job is finished
        if instance_id is not None:
            delete_instance(instance_id)
            logger.log_info(f"Deleted instance {instance_id}")

    # Forward the response from the instance
    return Response(content, status=status)


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
            # > $env:GCS_SERVICE_ACCOUNT = "github-open-covid-19@appspot.gserviceaccount.com"
            # > $env:GCP_TOKEN = $(gcloud auth application-default print-access-token)
            app.run(host="127.0.0.1", port=8080, debug=True)
        else:
            app.run(host="0.0.0.0", port=80, debug=False)

    def _publish():
        publish_tables()
        publish_main_table()
        publish_subset_tables()

    def _unknown_command(*func_args):
        logger.log_error(f"Unknown command {args.command}")

    # If a command + args are supplied, call the corresponding function
    {
        "server": _start_server,
        "update_table": update_table,
        "combine_table": combine_table,
        "cache_pull": cache_pull,
        "publish": _publish,
        "convert_json": _convert_json,
        "report_errors_to_github": report_errors_to_github,
    }.get(args.command, _unknown_command)(**json.loads(args.args or "{}"))


if __name__ == "__main__":
    main()
