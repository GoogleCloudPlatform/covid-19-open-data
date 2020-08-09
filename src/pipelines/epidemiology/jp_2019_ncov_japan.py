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
from pandas import DataFrame, concat
from lib.data_source import DataSource
from lib.time import datetime_isoformat
from lib.utils import pivot_table, table_multimerge


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


class Jp2019NcovJapanByDate(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Keep only columns we can process
        data = table_multimerge(
            [_parse_pivot(df, name) for name, df in dataframes.items()], how="outer"
        )
        data = data[["date", "country_code", "match_string", "new_confirmed", "new_deceased"]]
        return data.fillna(0)


# Unused because it's a different region aggregation
class Jp2019NcovJapanByRegion(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = dataframes[0].rename(
            columns={
                "日付": "date",
                "都道府県名": "match_string",
                "患者数": "new_confirmed",
                "入院中": "new_hospitalized",
                "退院者": "new_recovered",
                "死亡者": "new_deceased",
            }
        )

        # Convert date to ISO format
        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%Y%m%d"))

        # Add the country code to all records
        data["country_code"] = "JP"

        # Keep only columns we can process
        data = data[
            [
                "date",
                "match_string",
                "new_confirmed",
                "new_hospitalized",
                "new_recovered",
                "new_deceased",
            ]
        ]

        # Aggregate the country-level data
        data_country = data.groupby("date").sum().reset_index()
        data_country["key"] = "JP"

        # Output the results
        return concat([data_country, data])
