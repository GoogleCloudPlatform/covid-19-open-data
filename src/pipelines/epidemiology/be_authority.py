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
from lib.utils import table_rename


_column_adapter = {
    "DATE": "date",
    "PROVINCE": "match_string",
    "AGEGROUP": "age",
    "SEX": "sex",
    "CASES": "new_confirmed",
    "TESTS_ALL": "new_tested",
    "NEW_IN": "new_hospitalized",
    "TOTAL_IN": "current_hospitalized",
    "TOTAL_IN_ICU": "current_intensive_care",
    "TOTAL_IN_RESP": "current_ventilator",
}


class BelgiumDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(dataframes[0], _column_adapter, drop=True).dropna(subset=["date"])

        # Make sure date is in ISO format
        data["date"] = data["date"].astype(str).apply(lambda x: x[:10])

        # Some sheets don't have a province and are only country level
        if "match_string" not in data.columns:
            data["match_string"] = None

        # Aggregate to country level
        country = data.drop(columns=["match_string"]).groupby(["date"]).sum().reset_index()

        # Add the country code to all records
        country["key"] = "BE"
        data["country_code"] = "BE"

        return concat([country, data.dropna(subset=["match_string"])])
