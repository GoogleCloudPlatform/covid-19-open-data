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

from pathlib import Path
from typing import Any, Dict, List
from tempfile import TemporaryDirectory

from pandas import DataFrame
from lib.pipeline import DataSource
from .profiled_test_case import ProfiledTestCase


DUMMY_DATA_SOURCE_AUX = {"metadata": DataFrame()}
DUMMY_DATA_SOURCE_CONFIG = {
    "fetch": [
        {
            "url": "http://samplecsvs.s3.amazonaws.com/SalesJan2009.csv",
            "opts": {"skip_existing": False},
        }
    ]
}


class DummyDataSouce(DataSource):
    def __init__(self, config=None):
        super().__init__(config=DUMMY_DATA_SOURCE_CONFIG)

    def parse_dataframes(self, dataframes, aux, **parse_opts):
        return dataframes[0]


class TestSourceFetch(ProfiledTestCase):
    def test_fetch_download(self):
        src = DummyDataSouce()
        with TemporaryDirectory() as output_folder:
            output_folder = Path(output_folder)
            src.run(output_folder, {}, DUMMY_DATA_SOURCE_AUX)
            snapshot_files = (output_folder / "snapshot").glob("*.csv")
            self.assertEqual(1, len(list(snapshot_files)))

    def test_fetch_skip_existing(self):
        src = DummyDataSouce()
        original_fetch_func = src.fetch

        def monkey_patch_fetch(
            output_folder: Path, cache: Dict[str, str], fetch_opts: List[Dict[str, Any]]
        ):
            self.assertEqual(True, fetch_opts[0].get("opts", {}).get("skip_existing"))
            return original_fetch_func(output_folder, cache, fetch_opts)

        src.fetch = monkey_patch_fetch
        with TemporaryDirectory() as output_folder:
            output_folder = Path(output_folder)
            src.run(output_folder, {}, DUMMY_DATA_SOURCE_AUX, skip_existing=True)


if __name__ == "__main__":
    sys.exit(main())
