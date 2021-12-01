# Copyright 2021 Google LLC
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
from lib.cast import numeric_code_as_string
from lib.data_source import DataSource
from lib.utils import table_rename


class USALEEPDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(
            dataframes[0],
            {"e(0)": "life_expectancy", "STATE2KX": "state_code", "CNTY2KX": "county_code"},
            drop=True,
        )

        # Derive the FIPS subregion code from state and county codes
        data["state_code"] = data["state_code"].apply(lambda x: numeric_code_as_string(x, 2))
        data["county_code"] = data["county_code"].apply(lambda x: numeric_code_as_string(x, 3))
        data["subregion2_code"] = data["state_code"] + data["county_code"]

        # Subregion 2 code is unique among all locations so we can drop all other codes
        data = data.drop(columns=["state_code", "county_code"])

        # Data is more granular than county level, use a crude average for estimate
        data = data.groupby("subregion2_code").mean().reset_index()

        # Add country code to all records and return
        data["country_code"] = "US"
        return data
