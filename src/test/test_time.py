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

from lib.time import date_range

from .profiled_test_case import ProfiledTestCase


class TestTimeFunctions(ProfiledTestCase):
    def test_date_range(self):
        start = "2020-01-01"
        end = "2020-01-09"

        expected = [
            "2020-01-01",
            "2020-01-02",
            "2020-01-03",
            "2020-01-04",
            "2020-01-05",
            "2020-01-06",
            "2020-01-07",
            "2020-01-08",
            "2020-01-09",
        ]

        # Test normal case
        self.assertListEqual(list(date_range(start, end)), expected)

        # Test start > end
        with self.assertRaises(AssertionError):
            # pylint: disable=arguments-out-of-order
            list(date_range(end, start))

        # Test start == end
        self.assertListEqual(list(date_range(start, start)), [expected[0]])


if __name__ == "__main__":
    sys.exit(main())
