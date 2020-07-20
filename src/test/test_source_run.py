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
import traceback
from unittest import main
from pathlib import Path
from functools import partial
from tempfile import TemporaryDirectory

import requests
from lib.concurrent import process_map, thread_map
from lib.constants import CACHE_URL
from lib.data_source import DataSource
from lib.pipeline import DataPipeline
from lib.pipeline_tools import get_pipeline_names
from .profiled_test_case import ProfiledTestCase


# Arbitrarily chosen to make sure tests run in a reasonable time
METADATA_SAMPLE_SIZE = 24


def _test_data_source(
    pipeline_name: DataPipeline, data_source_idx: DataSource, random_seed: int = 0
):
    # Re-load the data pipeline and data source
    # It seems inefficient but it's necessary because we can't move these objects across
    # processes
    pipeline = DataPipeline.load(pipeline_name)
    data_source = pipeline.data_sources[data_source_idx]

    # Load the real cache files
    cache = requests.get("{}/sitemap.json".format(CACHE_URL)).json()

    data_source_name = data_source.__class__.__name__
    data_source_opts = data_source.config
    failure_message = (
        f"Data source run failed: {pipeline_name} {data_source_name} {data_source_opts}"
    )
    if data_source_opts.get("test", {}).get("skip"):
        return

    # Make a copy of all auxiliary files
    aux = {name: table.copy() for name, table in pipeline.auxiliary_tables.items()}

    # If we have a hint for the expected keys, use only those from metadata
    metadata_query = data_source_opts.get("test", {}).get("metadata_query")
    if metadata_query:
        aux["metadata"] = aux["metadata"].query(metadata_query)

    # Get a small sample of metadata, since we are testing for whether a source produces
    # _any_ output, not if the output is exhaustive
    sample_size = min(len(aux["metadata"]), METADATA_SAMPLE_SIZE)
    aux["metadata"] = aux["metadata"].sample(sample_size, random_state=random_seed)

    # Use a different temporary working directory for each data source
    with TemporaryDirectory() as output_folder:
        output_folder = Path(output_folder)
        try:
            output_data = data_source.run(output_folder, cache, aux)
        except Exception as exc:
            traceback.print_exc()
            raise RuntimeError(failure_message)

        # Run our battery of tests against the output data to ensure it looks correct

        # Data source has at least one row in output
        assert len(output_data) >= 1, failure_message

        # TODO: run more tests


def _test_data_pipeline(pipeline_name: str, random_seed: int = 0):

    # Load the data pipeline to get the number of data sources
    data_pipeline = DataPipeline.load(pipeline_name)

    # Load the data pipeline, iterate over each data source and run it to get its output
    pipeline_count = len(data_pipeline.data_sources)
    map_func = partial(_test_data_source, pipeline_name, random_seed=random_seed)
    _ = thread_map(map_func, range(pipeline_count), total=pipeline_count, max_workers=4)

    # Consume the results
    list(_)


class TestSourceRun(ProfiledTestCase):
    def test_dry_run_pipeline(self):
        """
        This test loads the real configuration for all sources in a pipeline, and runs them against
        a subset of the metadata for matching of keys. The subset of the metadata is chosen by
        running the provided `test.metadata_query` for the metadata auxiliary table or, if the
        query is not present, a random sample is selected instead.
        """
        list(process_map(_test_data_pipeline, list(get_pipeline_names()), max_workers=2))


if __name__ == "__main__":
    sys.exit(main())
