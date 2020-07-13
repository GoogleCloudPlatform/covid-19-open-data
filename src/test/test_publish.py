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
from typing import List

from pandas import DataFrame
from publish import make_main_table, copy_tables
from lib.constants import SRC
from .profiled_test_case import ProfiledTestCase


class TestPublish(ProfiledTestCase):
    def _spot_check_subset(
        self, data: DataFrame, key: str, columns: List[str], first_date: str
    ) -> None:
        subset = data.loc[key, ["date"] + columns]

        # The first date provided has non-null values
        self.assertGreaterEqual(first_date, subset.dropna(subset=columns, how="all").date.min())

        # More than 2/3 of the rows don't have any null values after the first date
        self.assertGreaterEqual(len(subset.dropna()), len(subset[subset.date > first_date]) * 2 / 3)

    def test_make_main_table(self):
        with TemporaryDirectory() as workdir:
            workdir = Path(workdir)

            # Copy all test tables into the temporary directory
            copy_tables(SRC / "test" / "data", workdir)

            # Create the main table
            main_table = make_main_table(workdir).set_index("key")

            # Define sets of columns to check
            epi_basic = ["new_confirmed", "total_confirmed", "new_deceased", "total_deceased"]

            # Spot check: Country of Andorra
            self._spot_check_subset(main_table, "AD", epi_basic, "2020-03-02")

            # Spot check: State of New South Wales
            self._spot_check_subset(main_table, "AU_NSW", epi_basic, "2020-01-25")

            # Spot check: Alachua County
            self._spot_check_subset(main_table, "US_FL_12001", epi_basic, "2020-03-10")


if __name__ == "__main__":
    sys.exit(main())
