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
from pathlib import Path
from unittest import main
from tempfile import TemporaryDirectory

from update import main as update_data
from .profiled_test_case import ProfiledTestCase


class TestUpdate(ProfiledTestCase):
    def test_update_only_pipeline(self):
        with TemporaryDirectory() as output_folder:
            output_folder = Path(output_folder)
            quick_pipeline_name = "index"  # Pick a pipeline that is quick to run
            update_data(output_folder, only=[quick_pipeline_name])
            self.assertSetEqual(
                set(subfolder.name for subfolder in output_folder.iterdir()),
                {"intermediate", "tables", "snapshot"},
            )

    def test_update_bad_pipeline_name(self):
        with TemporaryDirectory() as output_folder:
            output_folder = Path(output_folder)
            bad_pipeline_name = "does_not_exist"
            with self.assertRaises(AssertionError):
                update_data(output_folder, only=[bad_pipeline_name])
            with self.assertRaises(AssertionError):
                update_data(output_folder, exclude=[bad_pipeline_name])


if __name__ == "__main__":
    sys.exit(main())
