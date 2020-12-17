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

from typing import Dict
from pandas import DataFrame
from lib.case_line import convert_cases_to_time_series
from lib.cast import safe_int_cast
from lib.data_source import DataSource
from lib.time import datetime_isoformat
from lib.utils import table_rename


class NewZealandDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        cases = table_rename(
            dataframes[0],
            {
                "Report Date": "date_new_confirmed",
                # "Case Status": "_status",
                "Sex": "sex",
                "Age group": "age",
                # "DHB": "",
                # "Overseas travel": "",
            },
            drop=True,
        )
        cases["key"] = "NZ"
        cases["age"] = cases["age"].str.slice(0, 2).str.replace(" ", "").apply(safe_int_cast)
        data = convert_cases_to_time_series(cases, ["key"])
        return data
