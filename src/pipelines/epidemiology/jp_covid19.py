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
from lib.cast import safe_int_cast
from lib.data_source import DataSource
from lib.utils import table_rename


class JapanCovid19DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(
            dataframes[0],
            {
                "year": "year",
                "month": "month",
                "date": "day",
                "prefectureNameE": "match_string",
                "testedPositive": "total_confirmed",
                "peopleTested": "total_tested",
                "hospitalized": "total_hospitalized",
                "serious": "total_intensive_care",
                "discharged": "total_discharged",
                "deaths": "total_deceased",
            },
            drop=True,
        )

        # Add the country code to all records
        data["country_code"] = "JP"

        # Parse the date as ISO format
        data["year"] = data["year"].apply(lambda x: f"{safe_int_cast(x):04d}")
        data["month"] = data["month"].apply(lambda x: f"{safe_int_cast(x):02d}")
        data["day"] = data["day"].apply(lambda x: f"{safe_int_cast(x):02d}")
        data["date"] = data.apply(lambda x: f"{x['year']}-{x['month']}-{x['day']}", axis=1)
        data.drop(columns=["year", "month", "day"])

        # Convert all values to numeric
        for col in data.columns:
            if col.startswith("total"):
                data[col] = data[col].apply(safe_int_cast)

        return data
