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
This script schedules all the jobs to be dispatched to AppEngine.
"""

import os
import sys
from argparse import ArgumentParser
from functools import partial
from typing import List

from google.cloud import scheduler_v1
from google.cloud.scheduler_v1.types import AppEngineHttpTarget, Duration, Job

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# pylint: disable=wrong-import-position
from lib.constants import GCP_LOCATION, SRC
from lib.memory_efficient import table_read_column
from lib.pipeline_tools import get_pipelines


def _split_into_subsets(items: List[str], bin_count: int):
    """ Produce subsets of the given list divided into equal `bin_count` bins """
    bin_size = len(items) // bin_count
    for idx in range(bin_count - 1):
        yield items[bin_size * idx : bin_size * (idx + 1)]
    # The last bin might have up to `bin_size - 1` additional items
    yield items[bin_size * (bin_count - 1) :]


def clear_jobs(
    client: scheduler_v1.CloudSchedulerClient, project_id: str, location_id: str
) -> None:
    """ Delete all scheduled jobs """
    parent = client.location_path(project_id, location_id)
    for job in client.list_jobs(parent):
        client.delete_job(job.name)


def schedule_job(
    client: scheduler_v1.CloudSchedulerClient,
    project_id: str,
    location_id: str,
    time_zone: str,
    schedule: str,
    path: str,
) -> None:
    """ Schedules the given job for the specified project and location """
    # Create a Job to schedule
    target = AppEngineHttpTarget(relative_uri=path, http_method="GET")
    timeout = Duration(seconds=2 * 60 * 60)  # 2 hours.
    job = Job(
        app_engine_http_target=target,
        schedule=schedule,
        time_zone=time_zone,
        attempt_deadline=timeout,
    )

    # Schedule the Job we just created
    parent = client.location_path(project_id, location_id)
    client.create_job(parent, job)


def schedule_all_jobs(project_id: str, location_id: str, time_zone: str) -> None:
    """
    Clears all previously scheduled jobs and schedules all necessary jobs for the current
    configuration.
    """
    client = scheduler_v1.CloudSchedulerClient()

    # Create a custom method with our parameters for ease of use
    _schedule_job = partial(
        schedule_job,
        client=client,
        project_id=project_id,
        location_id=location_id,
        time_zone=time_zone,
    )

    # Clear all pre-existing jobs
    clear_jobs(client=client, project_id=project_id, location_id=location_id)

    # Read the list of all known locations, since we will be splitting some jobs based on that
    location_keys = list(table_read_column(SRC / "data" / "metadata.csv", "key"))

    # Cache pull job runs hourly
    _schedule_job(schedule="0 * * * *", path="/deferred/cache_pull")

    # Get new errors once a day at midday.
    _schedule_job(path="/deferred/report_errors_to_github", schedule="0 12 * * *")

    # Keep track of the different job groups to only output them once
    job_urls_seen = set()

    for data_pipeline in get_pipelines():
        # The job that combines data sources into a table runs hourly
        _schedule_job(
            path=f"/deferred/combine_table?table={data_pipeline.table}",
            # Offset by 15 minutes to let other hourly tasks finish
            schedule="15 * * * *",
        )

        for data_source in data_pipeline.data_sources:
            automation_opts = data_source.config.get("automation", {})

            # The job to pull each individual data source runs hourly unless specified otherwise
            job_sched = automation_opts.get("schedule", "0 * * * *")

            # If the job is deferred, then prepend the token to the path
            job_prefix = "/deferred" if automation_opts.get("deferred", True) else ""

            # Each data source has a job group. All data sources within the same job group are run
            # as part of the same job in series. The default job group is "default".
            job_group = automation_opts.get("job_group", "default")
            job_url = f"{job_prefix}/update_table?table={data_pipeline.table}&job_group={job_group}"

            if job_url not in job_urls_seen:
                job_urls_seen.add(job_url)
                _schedule_job(path=job_url, schedule=job_sched)

    ########
    # V2 publish jobs
    ########

    # The job that publishes combined tables into the prod bucket runs every 2 hours
    _schedule_job(
        # Run in a separate, preemptible instance
        path="/deferred/publish_tables",
        # Offset by 30 minutes to let other hourly tasks finish
        schedule="30 */2 * * *",
    )

    # The job that publishes aggregate outputs runs every 4 hours
    _schedule_job(
        # Run in a separate, preemptible instance
        path="/deferred/publish_main_table",
        # Offset by 60 minutes to let other hourly tasks finish
        schedule="0 1-23/4 * * *",
    )

    # The job that publishes breakdown outputs runs every 4 hours
    _schedule_job(
        path="/deferred/publish_subset_tables",
        # Offset by 90 minutes to run after publishing
        schedule="30 1-23/4 * * *",
    )

    # Converting the outputs to JSON is less critical but also slow so it's run separately
    _schedule_job(
        path=f"/deferred/publish_json_tables?prod_folder=v2",
        # Offset by 120 minutes to run after subset tables are published
        schedule="0 2-23/4 * * *",
    )
    for subset in _split_into_subsets(location_keys, bin_count=5):
        job_params = f"prod_folder=v2&location_key_from={subset[0]}&location_key_until={subset[-1]}"
        _schedule_job(
            path=f"/deferred/publish_json_locations?{job_params}",
            # Offset by 120 minutes to run after subset tables are published
            schedule="0 2-23/4 * * *",
        )

    # Publish an index of versions for each global table
    _schedule_job(
        path=f"/deferred/publish_versions?prod_folder=v2",
        # Run this job hourly
        schedule="0 * * * *",
    )

    ########
    # V3 publish jobs
    ########

    # Publish the global tables (with all location keys) every 2 hours
    _schedule_job(
        path="/deferred/publish_global_tables?prod_folder=v3",
        # Offset by 30 minutes to let other hourly tasks finish
        schedule="30 */2 * * *",
    )

    # Publish the latest subset for all tables every 2 hours
    _schedule_job(
        path="/deferred/publish_v3_latest_tables",
        # Offset by 60 minutes to execute after publish_v3_global_tables finishes
        schedule="0 1-23/2 * * *",
    )

    # Convert the global tables to JSON
    _schedule_job(
        path=f"/deferred/publish_json_tables?prod_folder=v3",
        # Offset by 60 minutes to execute after publish_v3_global_tables finishes
        schedule="0 1-23/2 * * *",
    )

    # Break down the outputs by location key every 2 hours, and execute the job in chunks
    for subset in _split_into_subsets(location_keys, bin_count=5):
        job_params = f"location_key_from={subset[0]}&location_key_until={subset[-1]}"
        _schedule_job(
            path=f"/deferred/publish_v3_location_subsets?{job_params}",
            # Offset by 60 minutes to execute after publish_v3_global_tables finishes
            schedule="0 1-23/2 * * *",
        )

    # Publish the main aggregated table every 2 hours
    _schedule_job(
        path="/deferred/publish_v3_main_table",
        # Offset by 90 minutes to execute after publish_v3_location_subsets finishes
        schedule="30 1-23/2 * * *",
    )

    # Publish outputs in JSON format every 2 hours, and execute the job in chunks
    for subset in _split_into_subsets(location_keys, bin_count=5):
        job_params = f"prod_folder=v3&location_key_from={subset[0]}&location_key_until={subset[-1]}"
        _schedule_job(
            path=f"/deferred/publish_json_locations?{job_params}",
            # Offset by 90 minutes to execute after publish_v3_location_subsets finishes
            schedule="30 1-23/2 * * *",
        )

    # Publish an index of versions for each global table
    _schedule_job(
        path=f"/deferred/publish_versions?prod_folder=v3",
        # Run this job hourly
        schedule="0 * * * *",
    )


if __name__ == "__main__":

    # Get default values from environment
    default_project = os.environ.get("GCP_PROJECT")
    default_location = os.environ.get("GCP_LOCATION", GCP_LOCATION)
    default_time_zone = os.environ.get("GCP_TIME_ZONE", "America/New_York")

    # Parse arguments from the command line
    argparser = ArgumentParser()
    argparser.add_argument("--project-id", type=str, default=default_project)
    argparser.add_argument("--location-id", type=str, default=default_location)
    argparser.add_argument("--time-zone", type=str, default=default_time_zone)
    args = argparser.parse_args()

    # Ensure project ID is not empty, since we don't have a default value for it
    assert args.project_id is not None, 'Argument "project-id" must not be empty'

    # Clear all preexisting jobs and schedule the new ones, this assumes the current code has
    # already been successfully deployed to GAE in a previous build step
    schedule_all_jobs(args.project_id, args.location_id, args.time_zone)
