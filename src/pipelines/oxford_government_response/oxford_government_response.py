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

import re
from typing import Dict
from pandas import DataFrame
from lib.data_source import DataSource
from lib.time import datetime_isoformat


class OxfordGovernmentResponseDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = dataframes[0]
        data = data.drop(columns=["CountryName", "ConfirmedCases", "ConfirmedDeaths"])
        data = data.drop(columns=[col for col in data.columns if col.endswith("_Notes")])
        data = data.drop(columns=[col for col in data.columns if col.endswith("_IsGeneral")])
        data["date"] = data["Date"].apply(lambda x: datetime_isoformat(x, "%Y%m%d"))

        # Drop redundant flag columns
        data = data.drop(columns=[col for col in data.columns if "_Flag" in col])

        # Join with ISO data to retrieve our known regions
        data = data.rename(columns={"CountryCode": "3166-1-alpha-3"}).merge(aux["country_codes"])
        data = data.rename(columns={"3166-1-alpha-2": "country_code"})

        # Make sure subregions are allowed to match this data
        data = data.drop(columns=["key"])
        data["subregion1_code"] = None
        data["subregion2_code"] = None
        data["locality_code"] = None
        subregion1_mask = data["RegionCode"].notna()
        data.loc[subregion1_mask, "subregion1_code"] = data.loc[
            subregion1_mask, "RegionCode"
        ].apply(lambda x: x.split("_")[-1])

        # Some region codes are not using the usual ISO standard
        hotfix_triplets = [("GB", "SCO", "SCT"), ("GB", "WAL", "WLS")]
        for country_code, subregion1_code, replacement in hotfix_triplets:
            country_code_mask = data["country_code"] == country_code
            subregion1_code_mask = data["subregion1_code"] == subregion1_code
            data.loc[country_code_mask & subregion1_code_mask, "subregion1_code"] = replacement

        # Use consistent naming convention for columns
        keep_columns = [
            "date",
            "country_code",
            "subregion1_code",
            "subregion2_code",
            "locality_code",
            "StringencyIndex",
        ]
        data = data[[col for col in data.columns if "_" in col or col in keep_columns]]
        data.columns = [
            col if col in keep_columns else col.split("_")[-1].lower() for col in data.columns
        ]
        data.columns = [re.sub(r"\s", "_", col) for col in data.columns]

        # Fix column typos
        data = data.rename(
            columns={
                "StringencyIndex": "stringency_index",
                "close_public_transport": "public_transport_closing",
                "debt/contract_relief": "debt_relief",
            }
        )

        # Remove unneeded columns and output
        return data.drop(columns=["wildcard"])
