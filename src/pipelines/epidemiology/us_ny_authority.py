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
from typing import Any, Dict, List
import numpy
from pandas import DataFrame, concat, merge
from lib.cast import safe_float_cast
from lib.io import read_file
from lib.pipeline import DataSource
from lib.time import datetime_isoformat
from lib.utils import table_multimerge, table_rename


def _parse_boro(data: DataFrame, column_prefix: str, fips: str) -> DataFrame:
    data = table_rename(
        data,
        {
            "DATE_OF_INTEREST": "date",
            f"{column_prefix}_CASE_COUNT": "new_confirmed",
            f"{column_prefix}_HOSPITALIZED_COUNT": "new_hospitalized",
            f"{column_prefix}_DEATH_COUNT": "new_deceased",
        },
    )
    data.date = data.date.apply(lambda x: datetime_isoformat(x, "%m/%d/%Y"))
    data["key"] = f"US_NY_{fips}"
    return data


class NYCHealthDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        nyc_boros = {
            "BX": "36005",  # Bronx
            "BK": "36047",  # Kings AKA Brooklyn
            "MN": "36061",  # New York County AKA Manhattan
            "QN": "36081",  # Queens
            "SI": "36085",  # Richmond AKA Staten Island
        }
        return concat([_parse_boro(dataframes[0], boro, fips) for boro, fips in nyc_boros.items()])
