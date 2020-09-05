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

import numpy
import pandas
from lib.cast import safe_int_cast

from .profiled_test_case import ProfiledTestCase

TEST_DATA_NULL = {
    "": None,
    "a": None,
    "1s": None,
    "1f": None,
    None: None,
    object(): None,
    numpy.nan: None,
    pandas.NA: None,
    lambda x: x: None,
}


class TestCastFunctions(ProfiledTestCase):
    def test_cast_int(self):

        test_data = {
            "1": 1,
            "1.0": 1,
            "-1": -1,
            "-1.0": -1,
            "+1": 1,
            "1e3": 1000,
            "−1": -1,  # Special character dash.
            **TEST_DATA_NULL,
        }

        for value, expected in test_data.items():
            result = safe_int_cast(value)
            self.assertEqual(result, expected, f"[{value}] Expected: {expected}. Found: {result}")


if __name__ == "__main__":
    sys.exit(main())
