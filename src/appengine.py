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
import gzip
import json
import os.path
import sys
import time
import traceback
from argparse import ArgumentParser
from functools import partial, wraps
from io import BytesIO
from pathlib import Path
from typing import Callable, Dict, List, Optional

import requests
import yaml
from flask import Flask, Response, request
from google.cloud import storage
from google.cloud.storage.blob import Blob
from google.oauth2.credentials import Credentials

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# pylint: disable=wrong-import-position
from publish import (
    copy_tables,
    convert_tables_to_json,
    create_table_subsets,
    merge_location_breakout_tables,
    merge_output_tables,
    publish_global_tables,
    publish_location_breakouts,
    publish_location_aggregates,
)
from scripts.cloud_error_processing import register_new_errors

from lib.cast import safe_int_cast
from lib.concurrent import thread_map
from lib.constants import (
    GCS_CONTAINER_ID,
    GCP_SELF_DESTRUCT_SCRIPT,
    GCS_BUCKET_PROD,
    GCS_BUCKET_TEST,
    SRC,
    V3_TABLE_LIST,
)
from lib.error_logger import ErrorLogger
from lib.gcloud import delete_instance, get_internal_ip, start_instance_from_image
from lib.io import export_csv, gzip_file, temporary_directory
from lib.memory_efficient import table_read_column
from lib.net import download
from lib.pipeline import DataPipeline
from lib.pipeline_tools import get_table_names

app = Flask(__name__)
logger = ErrorLogger("appengine")

BLOB_OP_MAX_RETRIES = 10
ENV_TOKEN = "GCP_TOKEN"
ENV_PROJECT = "GOOGLE_CLOUD_PROJECT"
ENV_SERVICE_ACCOUNT = "GCS_SERVICE_ACCOUNT"
COMPRESS_EXTENSIONS = ("json",)
# Used when parsing string parameters into boolean type
BOOL_STRING_MAP = {"true": True, "false": False, "1": True, "0": False, "": False, "null": False}


def _get_request_param(name: str, default: str = None) -> Optional[str]:
    try:
        return request.args.get(name, default)
    except:
        return default


def profiled_route(rule, **options):
    """ Decorator used to wrap @app.route and log metrics and response codes. """

    def decorator(func):

        # Define the wrapped function which profiles entry and exit points
        @wraps(func)
        def profiled_method_call(*args, **kwargs) -> Response:
            # Request arguments may not be available depending on context
            request_params = {}
            try:
                request_params = request.args
            except RuntimeError:
                pass

            # Start timer and log method entry
            time_start = time.monotonic()
            log_opts = {"handler": rule, "args": args, "kwargs": kwargs, "params": request_params}
            logger.log_info("enter", **log_opts)

            # Stop timer as soon as response is received
            response: Response = func(*args, **kwargs)
            time_elapsed = time.monotonic() - time_start

            # Log method exit as INFO or ERROR depending on response status code
            if response.status_code >= 200 and response.status_code < 400:
                log_func = logger.log_info
            else:
                log_func = logger.log_error
            log_func("exit", seconds=time_elapsed, status_code=response.status_code, **log_opts)

            # Relay response as return value for function
            return response

        # Register this route within the app's rules and return the wrapped function
        endpoint = options.pop("endpoint", None)
        app.add_url_rule(rule, endpoint, profiled_method_call, **options)

        # Return the wrapped function
        return profiled_method_call

    # Return the decorator
    return decorator


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
                    name, suffix = file_path.name, file_path.suffix

                    # If it's an extension we should compress, upload compressed file
                    if suffix[1:] in COMPRESS_EXTENSIONS:
                        with temporary_directory() as workdir:
                            gzipped_file = workdir / name
                            gzip_file(file_path, gzipped_file)
                            blob.content_encoding = "gzip"
                            return blob.upload_from_filename(gzipped_file)

                    # Otherwise upload the file as-is
                    else:
                        return blob.upload_from_filename(file_path)

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


@profiled_route("/cache_pull")
def cache_pull() -> Response:
    with temporary_directory() as workdir:
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


@profiled_route("/update_table")
def update_table(table_name: str = None, job_group: str = None, parallel_jobs: int = 8) -> Response:
    table_name = _get_request_param("table", table_name)
    job_group = _get_request_param("job_group", job_group)
    process_count = _get_request_param("parallel_jobs", parallel_jobs)
    # Default to 1 if invalid process count is given
    process_count = safe_int_cast(process_count) or 1

    # Early exit: table name not found
    if table_name not in list(get_table_names()):
        return Response(f"Invalid table name {table_name}", status=400)

    with temporary_directory() as workdir:
        (workdir / "snapshot").mkdir(parents=True, exist_ok=True)
        (workdir / "intermediate").mkdir(parents=True, exist_ok=True)

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
        logger.log_info(f"Updating data sources: {data_source_names}")

        # Produce the intermediate files from the data source
        intermediate_results = data_pipeline.parse(workdir, process_count=process_count)
        data_pipeline._save_intermediate_results(workdir / "intermediate", intermediate_results)
        intermediate_files = list(map(str, (workdir / "intermediate").glob("*.csv")))
        logger.log_info(f"Created intermediate tables: {intermediate_files}")

        # Upload results to the test bucket because these are not prod files
        upload_folder(GCS_BUCKET_TEST, "snapshot", workdir / "snapshot")
        upload_folder(GCS_BUCKET_TEST, "intermediate", workdir / "intermediate")

    return Response("OK", status=200)


@profiled_route("/combine_table")
def combine_table(table_name: str = None) -> Response:
    table_name = _get_request_param("table", table_name)
    logger.log_info(f"Combining data sources for {table_name}")

    # Early exit: table name not found
    if table_name not in list(get_table_names()):
        return Response(f"Invalid table name {table_name}", status=400)

    with temporary_directory() as workdir:
        (workdir / "tables").mkdir(parents=True, exist_ok=True)

        # Load the pipeline configuration given its name
        pipeline_name = table_name.replace("-", "_")
        data_pipeline = DataPipeline.load(pipeline_name)

        # Get a list of the intermediate files used by this data pipeline
        intermediate_file_names = []
        for data_source in data_pipeline.data_sources:
            intermediate_file_names.append(f"{data_source.uuid(data_pipeline.table)}.csv")
        logger.log_info(f"Downloading intermediate tables {intermediate_file_names}")

        # Download only the necessary intermediate files
        download_folder(
            GCS_BUCKET_TEST,
            "intermediate",
            workdir / "intermediate",
            lambda x: x.name in intermediate_file_names,
        )

        # Re-load all intermediate results
        intermediate_results = data_pipeline._load_intermediate_results(workdir / "intermediate")
        logger.log_info(f"Loaded intermediate tables {intermediate_file_names}")

        # Limit the number of processes to avoid OOM in big datasets
        process_count = 4

        # Combine all intermediate results into a single dataframe
        pipeline_output = data_pipeline.combine(intermediate_results, process_count=process_count)
        logger.log_info(f"Combined intermediate tables into {table_name}")

        # Output combined data to disk
        output_path = workdir / "tables" / f"{table_name}.csv"
        export_csv(pipeline_output, output_path, schema=data_pipeline.schema)
        logger.log_info(f"Exported combined {table_name} to CSV")

        # Upload results to the test bucket because these are not prod files
        # They will be copied to prod in the publish step, so main.csv is in sync
        logger.log_info(f"Uploading combined {table_name}...")
        upload_folder(GCS_BUCKET_TEST, "tables", workdir / "tables")

    return Response("OK", status=200)


@profiled_route("/publish_tables")
def publish_tables() -> Response:
    with temporary_directory() as workdir:
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


@profiled_route("/publish_main_table")
def publish_main_table() -> Response:
    with temporary_directory() as workdir:
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
        merge_output_tables(input_folder, main_table_path)
        logger.log_info("Main table created")

        # Upload the results to the prod bucket
        upload_folder(GCS_BUCKET_PROD, "v2", output_folder)

    return Response("OK", status=200)


@profiled_route("/publish_subset_tables")
def publish_subset_tables() -> Response:
    with temporary_directory() as workdir:
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


@profiled_route("/publish_v3_global_tables")
def publish_v3_global_tables() -> Response:
    with temporary_directory() as workdir:
        tables_folder = workdir / "tables"
        public_folder = workdir / "public"
        tables_folder.mkdir(parents=True, exist_ok=True)
        public_folder.mkdir(parents=True, exist_ok=True)

        # Download all the combined tables into our local storage
        download_folder(GCS_BUCKET_TEST, "tables", tables_folder)

        # Publish the tables containing all location keys
        publish_global_tables(tables_folder, public_folder, use_table_names=V3_TABLE_LIST)
        logger.log_info("Global tables created")

        # Upload the results to the prod bucket
        upload_folder(GCS_BUCKET_PROD, "v3", public_folder)

    return Response("OK", status=200)


@profiled_route("/publish_v3_location_subsets")
def publish_v3_location_subsets(
    location_key_from: str = None, location_key_until: str = None
) -> Response:
    location_key_from = _get_request_param("location_key_from", location_key_from)
    location_key_until = _get_request_param("location_key_until", location_key_until)

    with temporary_directory() as workdir:
        input_folder = workdir / "input"
        intermediate_folder = workdir / "temp"
        output_folder = workdir / "output"
        input_folder.mkdir(parents=True, exist_ok=True)
        output_folder.mkdir(parents=True, exist_ok=True)

        location_keys = list(table_read_column(SRC / "data" / "metadata.csv", "key"))
        if location_key_from is not None:
            location_keys = [key for key in location_keys if key >= location_key_from]
        if location_key_until is not None:
            location_keys = [key for key in location_keys if key <= location_key_until]
        logger.log_info(
            f"Publishing {len(location_keys)} location subsets "
            f"from {location_keys[0]} until {location_keys[-1]}"
        )

        # Download all the global tables into our local storage
        forbid_tokens = ("/", "main.", "aggregated.")
        download_folder(
            GCS_BUCKET_PROD,
            "v3",
            input_folder,
            lambda x: x.suffix == ".csv" and all(token not in str(x) for token in forbid_tokens),
        )
        logger.log_info(f"Downloaded {sum(1 for _ in input_folder.glob('**/*.csv'))} CSV files")

        # Break out each table into separate folders based on the location key
        publish_location_breakouts(input_folder, intermediate_folder, use_table_names=V3_TABLE_LIST)
        logger.log_info("Created all table location breakouts")

        # Create a folder which will host all the location aggregates
        location_aggregates_folder = output_folder / "location"
        location_aggregates_folder.mkdir(parents=True, exist_ok=True)

        # Aggregate the tables for each location independently
        publish_location_aggregates(
            intermediate_folder,
            location_aggregates_folder,
            location_keys,
            use_table_names=V3_TABLE_LIST,
        )
        logger.log_info("Aggregated all table breakouts by location")

        # Upload the results to the prod bucket
        upload_folder(GCS_BUCKET_PROD, "v3", output_folder)

    return Response("OK", status=200)


@profiled_route("/publish_v3_main_table")
def publish_v3_main_table() -> Response:
    with temporary_directory() as workdir:
        input_folder = workdir / "input"
        output_folder = workdir / "output"
        input_folder.mkdir(parents=True, exist_ok=True)
        output_folder.mkdir(parents=True, exist_ok=True)

        # Download all the location breakout tables into our local storage
        download_folder(GCS_BUCKET_PROD, "v3", input_folder, lambda x: "location/" in str(x))
        logger.log_info(f"Downloaded {sum(1 for _ in input_folder.glob('**/*.csv'))} CSV files")

        # Create the aggregated table and put it in a compressed file
        agg_file_path = output_folder / "aggregated.csv.gz"
        with gzip.open(agg_file_path, "wt") as compressed_file:
            merge_location_breakout_tables(input_folder, compressed_file)

        # Upload the results to the prod bucket
        upload_folder(GCS_BUCKET_PROD, "v3", output_folder)

    return Response("OK", status=200)


@profiled_route("/publish_json_locations")
def publish_json_locations(
    prod_folder: str = "v2", location_key_from: str = None, location_key_until: str = None
) -> Response:
    prod_folder = _get_request_param("prod_folder", prod_folder)
    location_key_from = _get_request_param("location_key_from", location_key_from)
    location_key_until = _get_request_param("location_key_until", location_key_until)

    with temporary_directory() as workdir:
        input_folder = workdir / "input"
        output_folder = workdir / "output"
        input_folder.mkdir(parents=True, exist_ok=True)
        output_folder.mkdir(parents=True, exist_ok=True)

        # Convert the tables to JSON for each location independently
        location_keys = list(table_read_column(SRC / "data" / "metadata.csv", "key"))
        if location_key_from is not None:
            location_keys = [key for key in location_keys if key >= location_key_from]
        if location_key_until is not None:
            location_keys = [key for key in location_keys if key <= location_key_until]
        logger.log_info(
            f"Converting {len(location_keys)} location subsets to JSON "
            f"from {location_keys[0]} until {location_keys[-1]}"
        )

        # Download all the processed tables into our local storage
        def match_path(table_path: Path) -> bool:
            try:
                if prod_folder == "v2":
                    location_key, table_name = str(table_path).split("/", 1)
                    return table_name == "main.csv" and location_key in location_keys
                elif prod_folder == "v3":
                    location_path, location_key = table_path.parent.name, table_path.stem
                    return location_path == "location" and location_key in location_keys
            except:
                return False

        download_folder(GCS_BUCKET_PROD, prod_folder, input_folder, match_path)
        logger.log_info(f"Downloaded {sum(1 for _ in input_folder.glob('**/*.csv'))} CSV files")

        # Convert all files to JSON
        convert_tables_to_json(input_folder, output_folder)
        converted_count = sum(1 for _ in output_folder.glob("**/*.json"))
        logger.log_info(f"Converted {converted_count} files to JSON")

        # Upload the results to the prod bucket
        upload_folder(GCS_BUCKET_PROD, prod_folder, output_folder)

    return Response("OK", status=200)


@profiled_route("/publish_json_tables")
def publish_json_tables(prod_folder: str = "v2") -> Response:
    prod_folder = _get_request_param("prod_folder", prod_folder)

    with temporary_directory() as workdir:
        input_folder = workdir / "input"
        output_folder = workdir / "output"
        input_folder.mkdir(parents=True, exist_ok=True)
        output_folder.mkdir(parents=True, exist_ok=True)

        # Download all the global tables into our local storage
        forbid_tokens = ("/", "main.", "aggregated.")
        download_folder(
            GCS_BUCKET_PROD,
            prod_folder,
            input_folder,
            lambda x: x.suffix == ".csv" and all(token not in str(x) for token in forbid_tokens),
        )
        logger.log_info(f"Downloaded {sum(1 for _ in input_folder.glob('**/*.csv'))} CSV files")

        # Convert all files to JSON
        convert_tables_to_json(input_folder, output_folder)
        logger.log_info("CSV files converted to JSON")

        # Upload the results to the prod bucket
        upload_folder(GCS_BUCKET_PROD, prod_folder, output_folder)

    return Response("OK", status=200)


@profiled_route("/report_errors_to_github")
def report_errors_to_github() -> Response:
    register_new_errors(os.getenv(ENV_PROJECT))
    return Response("OK", status=200)


@profiled_route("/deferred/<path:url_path>")
def deferred_route(url_path: str) -> Response:
    status, content = 500, "Unknown error"

    # Initialize the instance ID variable outside of the try so it's available at finally
    instance_id = None

    # Prevent chaining deferred calls
    if any([token == "deferred" for token in url_path.split("/")]):
        return Response(status=400)

    try:
        # Create a new preemptible instance and wait for it to come online
        instance_opts = dict(service_account=os.getenv(ENV_SERVICE_ACCOUNT))
        instance_id = start_instance_from_image(GCS_CONTAINER_ID, **instance_opts)
        instance_ip = get_internal_ip(instance_id)
        logger.log_info(f"Created worker instance {instance_id} with internal IP {instance_ip}")

        # Wait before attempting to forward the request
        max_retries = 8
        warmup_time = 30
        status_check_url = f"http://{instance_ip}/status_check"
        for _ in range(max_retries):
            try:
                response = requests.get(url=status_check_url, timeout=60)
                if response.ok and response.text == "OK":
                    break
            except requests.exceptions.ConnectionError:
                logger.log_info(f"Waiting {warmup_time} to let the instance warm up")
                time.sleep(warmup_time)

        # Forward the route to the worker instance
        params = dict(request.args)
        headers = dict(request.headers)
        url_fwd = f"http://{instance_ip}/{url_path}"
        log_opts = dict(path=url_path, params=params)
        logger.log_info(f"Forwarding request to {instance_id}: {instance_ip}", **log_opts)
        response = requests.get(url=url_fwd, headers=headers, params=params, timeout=60 * 60 * 2)

        # Retrieve the response from the worker instance
        status, content = response.status_code, response.content
        logger.log_info(f"Received response from {instance_id}", status=status, **log_opts)

    except requests.exceptions.Timeout:
        logger.log_error(f"Request to {instance_id} timed out", path=url_path)
        status, content = 500, "Timeout"

    except Exception as exc:
        log_opts = dict(traceback=traceback.format_exc(), path=url_path)
        logger.log_error(f"Exception while waiting for response from {instance_id}", **log_opts)
        status, content = 500, "Internal exception"

    finally:
        # Shut down the worker instance now that the job is finished
        if instance_id is not None:
            delete_instance(instance_id)
            logger.log_info(f"Deleted instance {instance_id}")

    # Forward the response from the instance
    return Response(content, status=status)


@profiled_route("/create_instance")
def create_instance(
    image_id: str = GCS_CONTAINER_ID, preemptible: str = "true", self_destruct: str = "true"
) -> Response:
    status, content = 500, "Unknown error"
    image_id = _get_request_param("image_id", image_id)
    preemptible_param = _get_request_param("preemptible", preemptible).lower()
    preemptible = BOOL_STRING_MAP.get(preemptible_param, False)
    self_destruct_param = _get_request_param("self_destruct", self_destruct).lower()
    self_destruct = BOOL_STRING_MAP.get(self_destruct_param, False)

    try:
        # Create a new preemptible instance and wait for it to come online
        instance_opts = dict(
            service_account=os.getenv(ENV_SERVICE_ACCOUNT),
            preemptible=preemptible,
            startup_script=str(GCP_SELF_DESTRUCT_SCRIPT) if self_destruct else None,
        )
        instance_id = start_instance_from_image(GCS_CONTAINER_ID, **instance_opts)
        instance_ip = get_internal_ip(instance_id)
        # Returns created instance details instead of the usual "OK"
        status, content = 200, json.dumps(dict(id=instance_id, **instance_opts))
        logger.log_info(f"Created worker instance {instance_id} with internal IP {instance_ip}")

    except Exception as exc:
        log_opts = dict(traceback=traceback.format_exc())
        logger.log_error(f"Exception creating instance from image ID {image_id}", **log_opts)
        status, content = 500, "Internal exception"

    # Forward the response from the instance
    return Response(content, status=status)


@app.route("/status_check")
def status_check() -> Response:
    # Simple response used to check the status of server
    return Response("OK", status=200)


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

    def _publish_v3():
        publish_v3_global_tables()
        publish_v3_location_subsets()

    def _publish_json(**kwargs):
        publish_json_tables()
        publish_json_locations(**kwargs)

    def _publish_json_v3(**kwargs):
        publish_json_tables(prod_folder="v3")
        publish_json_locations(prod_folder="v3", **kwargs)

    def _unknown_command(**kwargs):
        logger.log_error(f"Unknown command {args.command}")

    # If a command + args are supplied, call the corresponding function
    logger.log_info(f"Executing command {args.command} with args {args.args}")
    {
        "server": _start_server,
        "update_table": update_table,
        "combine_table": combine_table,
        "cache_pull": cache_pull,
        "publish": _publish,
        "convert_json": _publish_json,
        "publish_v3": _publish_v3,
        "publish_v3_json": _publish_json_v3,
        "publish_v3_main": publish_v3_main_table,
        "report_errors_to_github": report_errors_to_github,
    }.get(args.command, _unknown_command)(**json.loads(args.args or "{}"))


if __name__ == "__main__":
    main()
