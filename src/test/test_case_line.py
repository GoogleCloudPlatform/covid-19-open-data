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
from io import StringIO
from unittest import main

from pandas import read_csv
from lib.case_line import convert_cases_to_time_series
from .profiled_test_case import ProfiledTestCase


CASE_LINE_DATA_HEADER = "key,age,sex,ethnicity,date_new_confirmed,date_new_deceased"

CASE_LINE_DATA_SIMPLE = f"""{CASE_LINE_DATA_HEADER}
AA,18,M,black,2020-01-01,2020-02-01
AA,19,M,white,2020-01-02,2020-02-01
AA,20,M,latino,2020-01-03,2020-02-01
AA,21,M,native american,2020-01-04,2020-02-01
AA,22,F,black,2020-01-05,2020-02-01
AA,23,F,white,2020-01-06,2020-02-01
AA,24,F,latino,2020-01-07,2020-02-01
AA,25,F,native american,2020-01-08,2020-02-01
"""

CASE_LINE_DATA_NULL_DEATHS = f"""{CASE_LINE_DATA_HEADER}
AA,18,M,black,2020-01-01,
AA,19,M,white,2020-01-02,
AA,20,M,latino,2020-01-03,
AA,21,M,native american,2020-01-04,
AA,22,F,black,2020-01-05,
AA,23,F,white,2020-01-06,
AA,24,F,latino,2020-01-07,
AA,25,F,native american,2020-01-08,
"""

CASE_LINE_DATA_OTHER = f"""{CASE_LINE_DATA_HEADER}
AA,18,other,other,2020-01-01,2020-01-01
"""

CASE_LINE_DATA_UNKNOWN = f"""{CASE_LINE_DATA_HEADER}
AA,?,?,?,2020-01-01,2020-01-01
"""

CASE_LINE_DATA_NULL_VALUES = f"""{CASE_LINE_DATA_HEADER}
AA,,,,2020-01-01,2020-01-01
"""


class TestCaseLine(ProfiledTestCase):
    def test_convert_cases_to_time_series_simple(self):
        cases = read_csv(StringIO(CASE_LINE_DATA_SIMPLE))
        table = convert_cases_to_time_series(cases)
        confirmed = table[table.new_confirmed > 0]

        # There should be as many records as there are combinations of <key,age,sex,ethnicity,date>
        self.assertEqual(len(cases) * 2, len(table))

        # All lines in our test case indicate a confirmed case
        self.assertEqual(len(cases), table["new_confirmed"].sum())

        # All lines in our test case indicate a deceased case
        self.assertEqual(len(cases), table["new_deceased"].sum())

        # Half of our cases are male, and the other half are female
        self.assertEqual(len(table[table.sex == "male"]), len(table[table.sex == "female"]))

        # 2 cases are 10-19 and 6 are 20-29
        self.assertEqual(2, len(confirmed[confirmed.age == "10-19"]))
        self.assertEqual(6, len(confirmed[confirmed.age == "20-29"]))

    def test_convert_cases_to_time_series_null_deaths(self):
        cases = read_csv(StringIO(CASE_LINE_DATA_NULL_DEATHS))
        table = convert_cases_to_time_series(cases)

        # There should be as many records as there are combinations of <key,age,sex,ethnicity,date>
        self.assertEqual(len(cases), len(table))

        # All lines in our test case indicate a confirmed case
        self.assertEqual(len(cases), table["new_confirmed"].sum())

        # No lines in our test case indicate a deceased case
        self.assertEqual(0, table["new_deceased"].sum())

        # Half of our cases are male, and the other half are female
        self.assertEqual(len(table[table.sex == "male"]), len(table[table.sex == "female"]))

    def test_convert_cases_to_time_series_other_values(self):
        cases = read_csv(StringIO(CASE_LINE_DATA_OTHER))
        table = convert_cases_to_time_series(cases)
        self.assertSetEqual({"sex_other"}, set(table.sex))
        self.assertSetEqual({"ethnicity_other"}, set(table.ethnicity))

    def test_convert_cases_to_time_series_unknown_values(self):
        cases = read_csv(StringIO(CASE_LINE_DATA_UNKNOWN))
        table = convert_cases_to_time_series(cases)
        self.assertSetEqual({"age_unknown"}, set(table.age))
        self.assertSetEqual({"sex_unknown"}, set(table.sex))
        self.assertSetEqual({"ethnicity_unknown"}, set(table.ethnicity))

    def test_convert_cases_to_time_series_null_values(self):
        cases = read_csv(StringIO(CASE_LINE_DATA_NULL_VALUES))
        table = convert_cases_to_time_series(cases)
        self.assertSetEqual({"age_unknown"}, set(table.age))
        self.assertSetEqual({"sex_unknown"}, set(table.sex))
        self.assertSetEqual({"ethnicity_unknown"}, set(table.ethnicity))


if __name__ == "__main__":
    sys.exit(main())
