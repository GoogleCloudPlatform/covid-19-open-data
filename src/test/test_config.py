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
from unittest import main
from lib.pipeline_tools import get_pipelines
from .profiled_test_case import ProfiledTestCase


class TestConfigFiles(ProfiledTestCase):
    def test_tables_config(self):
        """
        This test needs to be periodically updated as we add or remove data sources to the table's
        config.yaml files. The main purpose of this test is to ensure that there are no drastic
        changes in the configuration files, such as an empty config.yaml.
        """
        expected_source_counts = {
            "epidemiology": 50,
            "hospitalizations": 20,
            "by-age": 10,
            "by-sex": 10,
        }

        for pipeline in get_pipelines():
            data_sources = pipeline.data_sources
            expected_count = expected_source_counts.get(pipeline.table, 1)
            self.assertGreaterEqual(len(data_sources), expected_count)

    def test_config_metadata(self):
        """
        This test verifies that all the required metadata is present in the data source config,
        including licensing information.
        """
        required_metadata = ["label", "website", "license", "license_url"]
        for pipeline in get_pipelines():
            for data_source in pipeline.data_sources:
                for meta in required_metadata:
                    err_msg = f"{meta} missing in {data_source.name} ({pipeline.name})"
                    self.assertIn(meta, data_source.config.keys(), err_msg)

    # TODO(owahltinez): Add tests for auxiliary tables, like metadata.csv


if __name__ == "__main__":
    sys.exit(main())
