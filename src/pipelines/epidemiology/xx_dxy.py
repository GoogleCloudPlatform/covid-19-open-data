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
from lib.data_source import DataSource
from lib.time import timezone_adjust


class DXYDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = dataframes[0]

        # Adjust 7 hour difference between China's GMT+8 and GMT+1
        data["date"] = data["updateTime"].apply(lambda date: timezone_adjust(date, 7))

        # Rename the appropriate columns
        data = data.rename(
            columns={
                "countryEnglishName": "country_name",
                "provinceEnglishName": "match_string",
                "province_confirmedCount": "total_confirmed",
                "province_deadCount": "total_deceased",
                "province_curedCount": "total_recovered",
            }
        )

        # Filter specific country data only
        data = data[data["country_name"] == parse_opts["country_name"]]

        # This is time series data, get only the last snapshot of each day
        data = (
            data.sort_values("updateTime")
            .groupby(["date", "country_name", "match_string"])
            .last()
            .reset_index()
        )

        keep_columns = [
            "date",
            "country_name",
            "match_string",
            "total_confirmed",
            "total_deceased",
            "total_recovered",
        ]
        return data[keep_columns]
