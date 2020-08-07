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

import numpy
from pandas import DataFrame
from lib.io import export_csv, read_file

from .profiled_test_case import ProfiledTestCase


class TestIOFunctions(ProfiledTestCase):
    def _test_reimport_csv_helper(self, data: numpy.ndarray, test_case: str):
        tmpfile = Path(f"{__file__}.csv")
        data1 = DataFrame(data)
        export_csv(data1, tmpfile)
        data2 = read_file(tmpfile)
        tmpfile.unlink()
        try:
            self.assertTrue(numpy.allclose(data1.values, data2.values), test_case)
        except:
            self.assertTrue(numpy.equal(data1.values, data2.values).all(), test_case)

    def test_reimport_csv(self):
        """
        When output columns have values larger than 1E8, we try to convert them to Int64. Since
        Int64 has some weirdness associated with it (specifically conversion to str type) we want
        to make sure that the values do not get coerced into the wrong thing when they are
        exported and re-imported, because this is a critical operation in the process of the
        data pipeline and the use of intermediate files.
        """
        random_matrix = numpy.random.randn(100, 100)

        # Simple random values
        self._test_reimport_csv_helper(random_matrix, "random values")

        # Non-numeric values
        non_numeric_matrix = numpy.copy(random_matrix)
        non_numeric_matrix = "_" + DataFrame(random_matrix).astype(str)
        self._test_reimport_csv_helper(non_numeric_matrix.values, "non-numeric")

        # Large numeric values
        large_value_matrix = numpy.copy(random_matrix) * 1e10
        self._test_reimport_csv_helper(large_value_matrix, "large values")


if __name__ == "__main__":
    sys.exit(main())
