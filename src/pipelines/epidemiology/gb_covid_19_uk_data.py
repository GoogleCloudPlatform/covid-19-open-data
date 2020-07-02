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

from typing import Dict, List
from pandas import DataFrame, concat, merge, NA
from lib.cast import safe_int_cast
from lib.pipeline import DataSource
from lib.utils import grouped_diff


class Covid19UkDataL3DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # County data is already in the format we want
        data = (
            dataframes[0]
            .rename(
                columns={
                    "Date": "date",
                    "Country": "subregion1_name",
                    "AreaCode": "subregion2_code",
                    "TotalCases": "confirmed",
                }
            )
            .drop(columns=["Area"])
            .dropna(subset=["subregion2_code"])
        )

        # Add subregion1 code to the data
        gb_meta = aux["metadata"]
        gb_meta = gb_meta[gb_meta["country_code"] == "GB"]
        gb_meta = gb_meta[gb_meta["subregion2_code"].isna()]
        country_map = {
            idx: code
            for idx, code in gb_meta.set_index("subregion1_name")["subregion1_code"].iteritems()
        }
        data["subregion1_code"] = data["subregion1_name"].apply(lambda x: country_map[x])

        # Manually build the key rather than doing automated merge for performance reasons
        data["key"] = "GB_" + data["subregion1_code"] + "_" + data["subregion2_code"]

        # Now that we have the key, we don't need any other non-value columns
        data = data[["date", "key", "confirmed"]]

        data["confirmed"] = data["confirmed"].apply(safe_int_cast).astype("Int64")
        data = grouped_diff(data, ["key", "date"])
        return data


class Covid19UkDataL2DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Aggregate indicator time series data into relational format
        records = []
        for idx, rows in dataframes[0].groupby(["Date", "Country"]):
            records.append(
                {
                    "date": idx[0],
                    "subregion1_name": idx[1],
                    **{
                        record.loc["Indicator"]: record.loc["Value"]
                        for _, record in rows.iterrows()
                    },
                }
            )

        data = DataFrame.from_records(records).rename(
            columns={"ConfirmedCases": "confirmed", "Deaths": "deceased", "Tests": "tested"}
        )

        for col in ("confirmed", "deceased", "tested"):
            data[col] = data[col].apply(safe_int_cast).astype("Int64")

        data = grouped_diff(data, ["subregion1_name", "date"])
        data.loc[data["subregion1_name"] == "UK", "subregion1_name"] = None
        data["subregion2_code"] = None
        data["country_code"] = "GB"
        return data


class Covid19UkDataL1DataSource(Covid19UkDataL2DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = super().parse_dataframes(dataframes, aux, **parse_opts)

        # Aggregate data to the country level
        data = data.groupby("date").sum().reset_index()
        data["key"] = "GB"
        return data
