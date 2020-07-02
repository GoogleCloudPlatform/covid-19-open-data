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

from pandas import DataFrame
from lib.pipeline import DataSource
from .profiled_test_case import ProfiledTestCase

# Synthetic data used for testing
TEST_AUX_DATA = DataFrame.from_records(
    [
        # Country with no subregions
        {
            "key": "AA",
            "country_code": "AA",
            "subregion1_code": None,
            "subregion2_code": None,
            "match_string": None,
        },
        # Country with one level-1 subregion
        {
            "key": "AB",
            "country_code": "AB",
            "subregion1_code": None,
            "subregion2_code": None,
            "match_string": None,
        },
        {
            "key": "AB_1",
            "country_code": "AB",
            "subregion1_code": "1",
            "subregion2_code": None,
            "match_string": None,
        },
        # Country with five level-1 subregions
        {
            "key": "AC",
            "country_code": "AC",
            "subregion1_code": None,
            "subregion2_code": None,
            "match_string": None,
        },
        {
            "key": "AC_1",
            "country_code": "AC",
            "subregion1_code": "1",
            "subregion2_code": None,
            "match_string": None,
        },
        {
            "key": "AC_2",
            "country_code": "AC",
            "subregion1_code": "2",
            "subregion2_code": None,
            "match_string": None,
        },
        {
            "key": "AC_3",
            "country_code": "AC",
            "subregion1_code": "3",
            "subregion2_code": None,
            "match_string": None,
        },
        {
            "key": "AC_4",
            "country_code": "AC",
            "subregion1_code": "4",
            "subregion2_code": None,
            "match_string": None,
        },
        {
            "key": "AC_5",
            "country_code": "AC",
            "subregion1_code": "5",
            "subregion2_code": None,
            "match_string": None,
        },
        # Country with one level-1 subregion and one level-2 subregion
        {
            "key": "AD",
            "country_code": "AD",
            "subregion1_code": None,
            "subregion2_code": None,
            "match_string": None,
        },
        {
            "key": "AD_1",
            "country_code": "AD",
            "subregion1_code": "1",
            "subregion2_code": None,
            "match_string": None,
        },
        {
            "key": "AD_1_1",
            "country_code": "AD",
            "subregion1_code": "1",
            "subregion2_code": "1",
            "match_string": None,
        },
        # Country with one level-1 subregion and five level-2 subregions
        {
            "key": "AE",
            "country_code": "AE",
            "subregion1_code": None,
            "subregion2_code": None,
            "match_string": None,
        },
        {
            "key": "AE_1",
            "country_code": "AE",
            "subregion1_code": "1",
            "subregion2_code": None,
            "match_string": None,
        },
        {
            "key": "AE_1_1",
            "country_code": "AE",
            "subregion1_code": "1",
            "subregion2_code": "1",
            "match_string": None,
        },
        {
            "key": "AE_1_2",
            "country_code": "AE",
            "subregion1_code": "1",
            "subregion2_code": "2",
            "match_string": None,
        },
        {
            "key": "AE_1_3",
            "country_code": "AE",
            "subregion1_code": "1",
            "subregion2_code": "3",
            "match_string": None,
        },
        {
            "key": "AE_1_4",
            "country_code": "AE",
            "subregion1_code": "1",
            "subregion2_code": "4",
            "match_string": None,
        },
        {
            "key": "AE_1_5",
            "country_code": "AE",
            "subregion1_code": "1",
            "subregion2_code": "5",
            "match_string": None,
        },
    ]
)


class TestSourceMerge(ProfiledTestCase):
    def test_merge_no_match(self):
        aux = TEST_AUX_DATA.copy()
        pipeline = DataSource()
        record = {"country_code": "__"}
        key = pipeline.merge(record, {"metadata": aux})
        self.assertTrue(key is None)

    def test_merge_by_key(self):
        aux = TEST_AUX_DATA.copy()
        pipeline = DataSource()
        record = {"key": "AE_1_2"}
        key = pipeline.merge(record, {"metadata": aux})
        self.assertEqual(key, record["key"])

    def test_merge_zero_subregions(self):
        aux = TEST_AUX_DATA.copy()
        pipeline = DataSource()
        record = {"country_code": "AA"}
        key = pipeline.merge(record, {"metadata": aux})
        self.assertEqual(key, "AA")

    def test_merge_one_subregion(self):
        aux = TEST_AUX_DATA.copy()
        pipeline = DataSource()

        record = {"country_code": "AB"}
        key = pipeline.merge(record, {"metadata": aux})
        self.assertTrue(key is None)

        record = {"country_code": "AB", "subregion1_code": None}
        key = pipeline.merge(record, {"metadata": aux})
        self.assertEqual(key, "AB")

        record = {"country_code": "AB", "subregion1_code": "1"}
        key = pipeline.merge(record, {"metadata": aux})
        self.assertEqual(key, "AB_1")

    def test_merge_null_vs_empty(self):
        aux = TEST_AUX_DATA.copy()
        pipeline = DataSource()

        # Only one record has null region1_code
        record = {"country_code": "AD", "subregion1_code": None}
        key = pipeline.merge(record, {"metadata": aux})
        self.assertEqual(key, "AD")

        # Empty means "do not compare" rather than "filter non-null"
        record = {"country_code": "AD", "subregion1_code": ""}
        key = pipeline.merge(record, {"metadata": aux})
        self.assertEqual(key, None)

        # There are multiple records that fit this merge, so it's ambiguous
        record = {"country_code": "AD", "subregion1_code": "1"}
        key = pipeline.merge(record, {"metadata": aux})
        self.assertEqual(key, None)

        # Match fails because subregion1_code is not null
        record = {"country_code": "AD", "subregion1_code": None, "subregion2_code": "1"}
        key = pipeline.merge(record, {"metadata": aux})
        self.assertEqual(key, None)

        # Match is exact so the merge is unambiguous
        record = {"country_code": "AD", "subregion1_code": "1", "subregion2_code": "1"}
        key = pipeline.merge(record, {"metadata": aux})
        self.assertEqual(key, "AD_1_1")

        # Even though we don't have subregion1_code, there's only one record that matches
        record = {"country_code": "AD", "subregion1_code": "", "subregion2_code": "1"}
        key = pipeline.merge(record, {"metadata": aux})
        self.assertEqual(key, "AD_1_1")


if __name__ == "__main__":
    sys.exit(main())
