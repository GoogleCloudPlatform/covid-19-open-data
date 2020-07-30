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


class Covid19UkDataL3DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # County data is already in the format we want
        data = (
            dataframes[0]
            .rename(
                columns={
                    "Date": "date",
                    "Country": "subregion1_name",
                    "AreaCode": "subregion2_code",
                    "TotalCases": "total_confirmed",
                }
            )
            .drop(columns=["Area"])
            .dropna(subset=["subregion2_code"])
        )

        # Add subregion1 code to the data
        gb_meta = aux["metadata"]
        gb_meta = gb_meta[gb_meta["country_code"] == "GB"]
        gb_meta = gb_meta.set_index("subregion1_name")["subregion1_code"].drop_duplicates()
        country_map = {idx: code for idx, code in gb_meta.iteritems()}
        data["subregion1_code"] = data["subregion1_name"].apply(country_map.get)

        # All subregion codes should be found but sometimes we only have a subset available when
        # the pipeline is run in a test environment
        data = data.dropna(subset=["subregion1_code"])

        # Manually build the key rather than doing automated merge for performance reasons
        data["key"] = "GB_" + data["subregion1_code"] + "_" + data["subregion2_code"]

        # Now that we have the key, we don't need any other non-value columns
        data = data[["date", "key", "total_confirmed"]]

        data["total_confirmed"] = data["total_confirmed"].apply(safe_int_cast).astype("Int64")
        return data


class Covid19UkDataL2DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
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
            columns={
                "ConfirmedCases": "total_confirmed",
                "Deaths": "total_deceased",
                "Tests": "total_tested",
            }
        )

        for col in ("total_confirmed", "total_deceased", "total_tested"):
            data[col] = data[col].apply(safe_int_cast).astype("Int64")

        data.loc[data["subregion1_name"] == "UK", "subregion1_name"] = None
        data["subregion2_code"] = None
        data["country_code"] = "GB"

        return data


class Covid19UkDataL1DataSource(Covid19UkDataL2DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = super().parse_dataframes(dataframes, aux, **parse_opts)

        # Aggregate data to the country level
        data = data.groupby("date").sum().reset_index()
        data["key"] = "GB"
        return data
