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

from typing import Any, Dict, List
from pandas import DataFrame, concat, merge
from lib.io import read_file
from lib.pipeline import DataSource
from lib.time import datetime_isoformat
from lib.utils import pivot_table, grouped_cumsum


class Jp2019NcovJapanByDate(DataSource):
    @staticmethod
    def _parse_pivot(data: DataFrame, name: str):

        # Remove bogus values
        data = data.iloc[:, :-4]

        # Convert date to ISO format
        data["date"] = data["date"].apply(lambda x: datetime_isoformat(str(x), "%Y%m%d"))
        data = pivot_table(data.set_index("date")).rename(
            columns={"value": name, "pivot": "match_string"}
        )

        # Add the country code to all records
        data["country_code"] = "JP"

        # Output the results
        return data

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        df1 = Jp2019NcovJapanByDate._parse_pivot(dataframes[0], "confirmed")
        df2 = Jp2019NcovJapanByDate._parse_pivot(dataframes[1], "deceased")

        # Keep only columns we can process
        data = merge(df1, df2)
        data = data[["date", "country_code", "match_string", "confirmed", "deceased"]]
        return grouped_cumsum(data, ["country_code", "match_string", "date"])


# Unused because it's a different region aggregation
class Jp2019NcovJapanByRegion(DataSource):
    # data_urls: List[str] = [
    #     "{}/detailByRegion.csv".format(_gh_base_url),
    # ]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = dataframes[0].rename(
            columns={
                "日付": "date",
                "都道府県名": "match_string",
                "患者数": "confirmed",
                "入院中": "hospitalized",
                "退院者": "recovered",
                "死亡者": "deceased",
            }
        )

        # Convert date to ISO format
        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%Y%m%d"))

        # Add the country code to all records
        data["country_code"] = "JP"

        # Keep only columns we can process
        data = data[["date", "match_string", "confirmed", "hospitalized", "recovered", "deceased"]]

        # Aggregate the region-level data
        data = grouped_cumsum(data, ["country_code", "match_string", "date"])

        # Aggregate the country-level data
        data_country = data.groupby("date").sum().reset_index()
        data_country["key"] = "JP"

        # Output the results
        return concat([data_country, data])
