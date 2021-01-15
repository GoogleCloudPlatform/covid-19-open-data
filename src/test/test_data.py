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

from lib.constants import SRC
from lib.io import isna, read_file, read_lines

from .profiled_test_case import ProfiledTestCase

METADATA_PATH = SRC / "data" / "metadata.csv"


class TestCastFunctions(ProfiledTestCase):
    def _test_lexicographical_order(self, file_path: Path):
        last_line = ""
        for idx, line in enumerate(read_lines(file_path)):
            if idx > 0:
                key1 = line.split(",", 1)[0].replace("_", "-")
                key2 = last_line.split(",", 1)[0].replace("_", "-")
                msg = f"Keys in {file_path.name} must follow lexicographical order: {key1} ≤ {key2}"
                self.assertGreater(key1, key2, msg)
                last_line = line

    def test_sorted_data_files(self):
        self._test_lexicographical_order(METADATA_PATH)
        self._test_lexicographical_order(SRC / "data" / "census.csv")
        self._test_lexicographical_order(SRC / "data" / "localities.csv")
        self._test_lexicographical_order(SRC / "data" / "knowledge_graph.csv")

    def test_key_build(self):
        skip_keys = ("UA_40", "UA_43")
        metadata = read_file(METADATA_PATH).set_index("key")
        localities = read_file(SRC / "data" / "localities.csv")["locality"].unique()
        for key, record in metadata.iterrows():
            msg = f"{key} does not match region codes in metadata"
            tokens = key.split("_")
            if key in skip_keys:
                continue
            elif len(tokens) == 1:
                self.assertEqual(key, record["country_code"], msg)
            elif key in localities or not isna(record["locality_code"]):
                self.assertEqual(tokens[-1], record["locality_code"], msg)
            elif len(tokens) == 2:
                self.assertEqual(tokens[0], record["country_code"], msg)
                self.assertEqual(tokens[1], record["subregion1_code"], msg)
            elif len(tokens) == 3:
                self.assertEqual(tokens[0], record["country_code"], msg)
                self.assertEqual(tokens[1], record["subregion1_code"], msg)
                self.assertEqual(tokens[2], record["subregion2_code"], msg)


if __name__ == "__main__":
    sys.exit(main())
