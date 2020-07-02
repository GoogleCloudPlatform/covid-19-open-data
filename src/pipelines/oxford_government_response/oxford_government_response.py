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
from typing import Any, Dict, List, Tuple
from pandas import DataFrame, Int64Dtype, merge
from lib.cast import safe_int_cast
from lib.pipeline import DataSource, DataSource, DataPipeline
from lib.time import datetime_isoformat
from lib.utils import ROOT


class OxfordGovernmentResponseDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = dataframes[0]
        data = data.drop(columns=["CountryName", "ConfirmedCases", "ConfirmedDeaths"])
        data = data.drop(columns=[col for col in data.columns if col.endswith("_Notes")])
        data = data.drop(columns=[col for col in data.columns if col.endswith("_IsGeneral")])
        data["date"] = data["Date"].apply(lambda x: datetime_isoformat(x, "%Y%m%d"))

        # Drop redundant flag columns
        data = data.drop(columns=[col for col in data.columns if "_Flag" in col])

        # Join with ISO data to retrieve our key
        data = data.rename(columns={"CountryCode": "3166-1-alpha-3"}).merge(aux["country_codes"])

        # Use consistent naming convention for columns
        data = data[
            [col for col in data.columns if "_" in col or col in ("date", "key", "StringencyIndex")]
        ]
        data.columns = [col.split("_")[-1].lower() for col in data.columns]
        data.columns = [re.sub(r"\s", "_", col) for col in data.columns]

        # Fix column typos
        data = data.rename(
            columns={
                "stringencyindex": "stringency_index",
                "close_public_transport": "public_transport_closing",
                "debt/contract_relief": "debt_relief",
            }
        )

        # Remove unneeded columns
        data = data.drop(columns=["wildcard"])

        # Reorder columns and output result
        first_columns = ["date", "key"]
        data = data[first_columns + [col for col in data.columns if col not in first_columns]]
        return data.sort_values(first_columns)
