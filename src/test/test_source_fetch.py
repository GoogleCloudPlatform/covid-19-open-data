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

from pandas import DataFrame
from lib.data_source import DataSource
from lib.io import temporary_directory
from .profiled_test_case import ProfiledTestCase


DUMMY_DATA_KEY = "AAA"
DUMMY_DATA_SOURCE_AUX = {
    "metadata": DataFrame.from_records([{"key": DUMMY_DATA_KEY}]),
    "localities": DataFrame(columns=["key", "locality"]),
}
DUMMY_DATA_SOURCE_CONFIG = {
    "fetch": [
        {
            "url": "https://raw.githubusercontent.com/GoogleCloudPlatform/covid-19-open-data/main/src/test/data/epidemiology.csv"
        }
    ]
}


class DummyDataSouce(DataSource):
    def __init__(self, config=None):
        super().__init__(config=DUMMY_DATA_SOURCE_CONFIG)

    def parse_dataframes(self, dataframes, aux, **parse_opts):
        data = dataframes[0]
        data["key"] = DUMMY_DATA_KEY
        return data


class TestSourceFetch(ProfiledTestCase):
    def test_fetch_download(self):
        src = DummyDataSouce()
        with temporary_directory() as workdir:
            src.run(workdir, {}, DUMMY_DATA_SOURCE_AUX)
            snapshot_files = (workdir / "snapshot").glob("*.csv")
            self.assertEqual(1, len(list(snapshot_files)))

    def test_fetch_skip_existing(self):
        src = DummyDataSouce()
        with temporary_directory() as workdir:
            src.run(workdir, {}, DUMMY_DATA_SOURCE_AUX, skip_existing=True)


if __name__ == "__main__":
    sys.exit(main())
