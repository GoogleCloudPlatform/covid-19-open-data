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
from lib.cast import age_group, safe_int_cast

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

    def test_age_group_standard(self):
        self.assertEqual("0-9", age_group(0, bin_size=10, age_cutoff=90))
        self.assertEqual("0-9", age_group(0.0, bin_size=10, age_cutoff=90))
        self.assertEqual("0-9", age_group(9, bin_size=10, age_cutoff=90))
        self.assertEqual("10-19", age_group(10, bin_size=10, age_cutoff=90))
        self.assertEqual("10-19", age_group(19, bin_size=10, age_cutoff=90))
        self.assertEqual("90-", age_group(90, bin_size=10, age_cutoff=90))
        self.assertEqual("90-", age_group(100, bin_size=10, age_cutoff=90))
        self.assertEqual("90-", age_group(110, bin_size=10, age_cutoff=90))
        self.assertEqual("90-", age_group(1e9, bin_size=10, age_cutoff=90))
        self.assertRaises(ValueError, lambda: age_group(-1, bin_size=10, age_cutoff=90))
        self.assertRaises(ValueError, lambda: age_group(None, bin_size=10, age_cutoff=90))
        self.assertRaises(ValueError, lambda: age_group(numpy.nan, bin_size=10, age_cutoff=90))

    def test_age_group_different_bins(self):
        self.assertEqual("0-9", age_group(0, bin_size=10, age_cutoff=10))
        self.assertEqual("0-9", age_group(0, bin_size=10, age_cutoff=70))
        self.assertEqual("0-9", age_group(0, bin_size=10, age_cutoff=100))
        self.assertEqual("10-19", age_group(10, bin_size=10, age_cutoff=70))
        self.assertEqual("10-19", age_group(10, bin_size=10, age_cutoff=100))
        self.assertEqual("10-", age_group(10, bin_size=10, age_cutoff=10))
        self.assertEqual("70-", age_group(70, bin_size=10, age_cutoff=70))
        self.assertEqual("100-", age_group(100, bin_size=10, age_cutoff=100))


if __name__ == "__main__":
    sys.exit(main())
