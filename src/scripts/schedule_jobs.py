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
from functools import partial
from argparse import ArgumentParser

from google.cloud import scheduler_v1
from google.cloud.scheduler_v1.types import AppEngineHttpTarget, Duration, Job

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# pylint: disable=wrong-import-position
from lib.pipeline_tools import get_pipelines


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

    # Cache pull job runs hourly
    _schedule_job(schedule="0 * * * *", path="/cache_pull")

    # The job that publishes data into the prod bucket runs every 4 hours
    _schedule_job(
        path="/publish",
        # Offset by 30 minutes to let other hourly tasks finish
        schedule="30 */4 * * *",
    )

    # Converting the outputs to JSON is less critical but also slow so it's run separately
    _schedule_job(
        path="/convert_json_1",
        # Offset by 30 minutes to run after publishing
        schedule="0 1-23/4 * * *",
    )

    # The convert to JSON task is split in two because otherwise it takes too long
    _schedule_job(
        path="/convert_json_2",
        # Offset by 30 minutes to run after publishing
        schedule="0 1-23/4 * * *",
    )

    # Get new errors once a day at midday.
    _schedule_job(path="/report_errors_to_github", schedule="0 12 * * *")

    # Keep track of the different job groups to only output them once
    job_urls_seen = set()

    for data_pipeline in get_pipelines():
        # The job that combines data sources into a table runs hourly
        _schedule_job(
            path=f"/combine_table?table={data_pipeline.table}",
            # Offset by 15 minutes to let other hourly tasks finish
            schedule="15 * * * *",
        )

        for idx, data_source in enumerate(data_pipeline.data_sources):
            # The job to pull each individual data source runs hourly unless specified otherwise
            job_sched = data_source.config.get("automation", {}).get("schedule", "0 * * * *")

            # Each data source has a job group. All data sources within the same job group are run
            # as part of the same job in series. The default job group is the index of the data
            # source.
            job_group = data_source.config.get("automation", {}).get("job_group", idx)
            job_url = f"/update_table?table={data_pipeline.table}&job_group={job_group}"

            if job_url not in job_urls_seen:
                job_urls_seen.add(job_url)
                _schedule_job(path=job_url, schedule=job_sched)


if __name__ == "__main__":

    # Get default values from environment
    default_project = os.environ.get("GCP_PROJECT")
    default_location = os.environ.get("GCP_LOCATION", "us-east1")
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
