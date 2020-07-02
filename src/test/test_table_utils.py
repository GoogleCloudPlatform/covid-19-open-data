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

import numpy
from pandas import DataFrame, isnull
from lib.cast import age_group
from lib.utils import combine_tables, stack_table, infer_new_and_total
from .profiled_test_case import ProfiledTestCase

# Synthetic data used for testing
COMBINE_TEST_DATA_1 = DataFrame.from_records(
    [
        # Both values are null
        {"key": "A", "value_column_1": None, "value_column_2": None},
        # Left value is null
        {"key": "A", "value_column_1": None, "value_column_2": 1},
        # Right value is null
        {"key": "A", "value_column_1": 1, "value_column_2": None},
        # No value is null
        {"key": "A", "value_column_1": 1, "value_column_2": 1},
    ]
)
COMBINE_TEST_DATA_2 = DataFrame.from_records(
    [
        # Both values are null
        {"key": "A", "value_column_1": None, "value_column_2": None},
        # Left value is null
        {"key": "A", "value_column_1": None, "value_column_2": 2},
        # Right value is null
        {"key": "A", "value_column_1": 2, "value_column_2": None},
        # No value is null
        {"key": "A", "value_column_1": 2, "value_column_2": 2},
    ]
)
STACK_TEST_DATA = DataFrame.from_records(
    [
        {"idx": 0, "piv": "A", "val": 1},
        {"idx": 0, "piv": "B", "val": 2},
        {"idx": 1, "piv": "A", "val": 3},
        {"idx": 1, "piv": "B", "val": 4},
    ]
)
NEW_AND_TOTAL_TEST_DATA = DataFrame.from_records(
    [
        {"key": "A", "date": "2020-01-01", "new_value_column": 1, "total_value_column": 1},
        {"key": "A", "date": "2020-01-02", "new_value_column": 5, "total_value_column": 6},
        {"key": "A", "date": "2020-01-03", "new_value_column": -1, "total_value_column": 5},
        {"key": "A", "date": "2020-01-04", "new_value_column": 10, "total_value_column": 15},
        {"key": "B", "date": "2020-01-01", "new_value_column": 10, "total_value_column": 10},
        {"key": "B", "date": "2020-01-02", "new_value_column": -1, "total_value_column": 9},
        {"key": "B", "date": "2020-01-03", "new_value_column": 5, "total_value_column": 14},
        {"key": "B", "date": "2020-01-04", "new_value_column": 1, "total_value_column": 15},
    ]
)


class TestTableUtils(ProfiledTestCase):
    def test_combine_all_none(self):
        data1 = COMBINE_TEST_DATA_1.copy()
        data2 = COMBINE_TEST_DATA_2.copy()
        result = combine_tables([data1[0:1], data2[0:1]], ["key"])
        self.assertEqual(1, len(result))
        self.assertTrue(isnull(result.loc[0, "value_column_1"]))
        self.assertTrue(isnull(result.loc[0, "value_column_2"]))

    def test_combine_first_both_none(self):
        data1 = COMBINE_TEST_DATA_1.copy()
        data2 = COMBINE_TEST_DATA_2.copy()
        result = combine_tables([data1[0:1], data2[3:4]], ["key"])
        self.assertEqual(1, len(result))
        self.assertEqual(2, result.loc[0, "value_column_1"])
        self.assertEqual(2, result.loc[0, "value_column_2"])

    def test_combine_second_both_none(self):
        data1 = COMBINE_TEST_DATA_1.copy()
        data2 = COMBINE_TEST_DATA_2.copy()
        result = combine_tables([data1[3:4], data2[0:1]], ["key"])
        self.assertEqual(1, len(result))
        self.assertEqual(1, result.loc[0, "value_column_1"])
        self.assertEqual(1, result.loc[0, "value_column_2"])

    def test_combine_first_left_none(self):
        data1 = COMBINE_TEST_DATA_1.copy()
        data2 = COMBINE_TEST_DATA_2.copy()
        result = combine_tables([data1[1:2], data2[3:4]], ["key"])
        self.assertEqual(1, len(result))
        self.assertEqual(2, result.loc[0, "value_column_1"])
        self.assertEqual(2, result.loc[0, "value_column_2"])

    def test_combine_first_right_none(self):
        data1 = COMBINE_TEST_DATA_1.copy()
        data2 = COMBINE_TEST_DATA_2.copy()
        result = combine_tables([data1[2:3], data2[3:4]], ["key"])
        self.assertEqual(1, len(result))
        self.assertEqual(2, result.loc[0, "value_column_1"])
        self.assertEqual(2, result.loc[0, "value_column_2"])

    def test_combine_second_left_none(self):
        data1 = COMBINE_TEST_DATA_1.copy()
        data2 = COMBINE_TEST_DATA_2.copy()
        result = combine_tables([data1[3:4], data2[1:2]], ["key"])
        self.assertEqual(1, len(result))
        self.assertEqual(1, result.loc[0, "value_column_1"])
        self.assertEqual(2, result.loc[0, "value_column_2"])

    def test_combine_second_right_none(self):
        data1 = COMBINE_TEST_DATA_1.copy()
        data2 = COMBINE_TEST_DATA_2.copy()
        result = combine_tables([data1[3:4], data2[2:3]], ["key"])
        self.assertEqual(1, len(result))
        self.assertEqual(2, result.loc[0, "value_column_1"])
        self.assertEqual(1, result.loc[0, "value_column_2"])

    def test_stack_data(self):
        expected = DataFrame.from_records(
            [
                {"idx": 0, "val": 3, "val_A": 1, "val_B": 2},
                {"idx": 1, "val": 7, "val_A": 3, "val_B": 4},
            ]
        )

        buffer1 = StringIO()
        buffer2 = StringIO()

        expected.to_csv(buffer1)
        stack_table(
            STACK_TEST_DATA, index_columns=["idx"], value_columns=["val"], stack_columns=["piv"]
        ).to_csv(buffer2)

        self.assertEqual(buffer1.getvalue(), buffer2.getvalue())

    def test_age_group(self):
        self.assertEqual("0-9", age_group(0, bin_count=10, max_age=100))
        self.assertEqual("0-9", age_group(0.0, bin_count=10, max_age=100))
        self.assertEqual("0-9", age_group(9, bin_count=10, max_age=100))
        self.assertEqual("10-19", age_group(10, bin_count=10, max_age=100))
        self.assertEqual("10-19", age_group(19, bin_count=10, max_age=100))
        self.assertEqual("90-", age_group(90, bin_count=10, max_age=100))
        self.assertEqual("90-", age_group(100, bin_count=10, max_age=100))
        self.assertEqual("90-", age_group(1e9, bin_count=10, max_age=100))
        self.assertEqual(None, age_group(-1, bin_count=10, max_age=100))

    def test_infer_nothing(self):

        # Ensure that no columns are added when both new_* and total_* are present
        self.assertSetEqual(
            set(NEW_AND_TOTAL_TEST_DATA.columns),
            set(["key", "date", "new_value_column", "total_value_column"]),
        )

        # Infer all missing new_ and total_ values, which should be none
        inferred_data = infer_new_and_total(NEW_AND_TOTAL_TEST_DATA, index_schema={"key": "str"})

        # Ensure that only the expected columns (and all the expected columns) are present
        self.assertSetEqual(
            set(inferred_data.columns),
            set(["key", "date", "new_value_column", "total_value_column"]),
        )

    def test_infer_total_from_new(self):

        # Ensure that total can be inferred from new_* values
        new_only_data = DataFrame.from_records(
            [
                {k: v for k, v in row.items() if "total" not in k}
                for _, row in NEW_AND_TOTAL_TEST_DATA.iterrows()
            ]
        )

        # Assert that only the new_* columns + index have been filtered
        self.assertSetEqual(set(new_only_data.columns), set(["key", "date", "new_value_column"]))

        # Compute the total_* values from new_*
        inferred_data = infer_new_and_total(new_only_data, index_schema={"key": "str"})

        # Ensure that only the expected columns (and all the expected columns) are present
        self.assertSetEqual(
            set(inferred_data.columns),
            set(["key", "date", "new_value_column", "total_value_column"]),
        )

        # Compare the result with the expected values
        inferred_total_values = inferred_data.total_value_column
        expected_total_values = NEW_AND_TOTAL_TEST_DATA.total_value_column
        self.assertListEqual(inferred_total_values.to_list(), expected_total_values.to_list())

    def test_infer_new_from_total(self):

        # Ensure that total can be inferred from new_* values
        new_only_data = DataFrame.from_records(
            [
                {k: v for k, v in row.items() if "new" not in k}
                for _, row in NEW_AND_TOTAL_TEST_DATA.iterrows()
            ]
        )

        # Assert that only the total_* columns + index have been filtered
        self.assertSetEqual(set(new_only_data.columns), set(["key", "date", "total_value_column"]))

        # Compute the total_* values from new_*
        inferred_data = infer_new_and_total(new_only_data, index_schema={"key": "str"})

        # Ensure that only the expected columns (and all the expected columns) are present
        self.assertSetEqual(
            set(inferred_data.columns),
            set(["key", "date", "new_value_column", "total_value_column"]),
        )

        # We can't infer new_* for the first value!
        test_data = NEW_AND_TOTAL_TEST_DATA.copy()
        test_data.loc[test_data.date == "2020-01-01", "new_value_column"] = numpy.nan
        expected_new_values = test_data.new_value_column

        # Compare the result with the expected values
        inferred_new_values = inferred_data.new_value_column
        # Workaround to remove nans because nan != nan
        self.assertListEqual(
            inferred_new_values.dropna().to_list(), expected_new_values.dropna().to_list()
        )

    # TODO: Add test for complex infer example (e.g. missing values)
    # TODO: Add test for stratify_age_and_sex


if __name__ == "__main__":
    sys.exit(main())
