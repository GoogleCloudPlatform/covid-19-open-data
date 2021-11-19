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

import json
from typing import Dict
from pandas import DataFrame, concat
from lib.case_line import convert_cases_to_time_series
from lib.pipeline import DataSource
from lib.time import datetime_isoformat
from lib.utils import table_rename, aggregate_admin_level


_column_adapter = {
    "txn_date": "date",
    "new_case": "new_confirmed",
    "total_case": "total_confirmed",
    "new_death": "new_deceased",
    "total_death": "total_deceased",
    "new_recovered": "new_recovered",
    "total_recovered": "total_recovered",
    "province": "match_string",
}


class ThailandCountryDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(dataframes[0], _column_adapter, drop=True)

        # Add key and return data
        data["key"] = "TH"
        return data


class ThailandProvinceDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(dataframes[0], _column_adapter, drop=True)

        # Add country code and return data
        data["country_code"] = "TH"
        return data


class ThailandCasesDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        cases = table_rename(
            dataframes[0],
            {
                # "no": "",
                "age": "age",
                "sex": "sex",
                # "nationality": "",
                # "province_of_isolation": "",
                # "notification_date": "date",
                "announce_date": "date_new_confirmed",
                "province_of_onset": "match_string",
                # "district_of_onset": "subregion2_name",
                # "quarantine": "",
            },
            drop=True,
            remove_regex=r"[^0-9a-z\s]",
        )

        # Convert date to ISO format
        cases["date_new_confirmed"] = cases["date_new_confirmed"].astype(str).str.slice(0, 10)

        # Translate sex labels; only male, female and unknown are given
        sex_adapter = lambda x: {"ชาย": "male", "หญิง": "female"}.get(x, "sex_unknown")
        cases["sex"] = cases["sex"].apply(sex_adapter)

        # Convert from cases to time-series format
        data = convert_cases_to_time_series(cases, ["match_string"])

        # Aggregate country-level data by adding all provinces
        country = (
            data.drop(columns=["match_string"]).groupby(["date", "age", "sex"]).sum().reset_index()
        )
        country["key"] = "TH"

        # Drop bogus records from the data
        data = data[data["match_string"].notna() & (data["match_string"] != "")]

        return concat([country, data])
