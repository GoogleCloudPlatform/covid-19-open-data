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

from typing import Any, Dict
from pandas import DataFrame
from lib.cast import safe_int_cast
from lib.data_source import DataSource
from lib.utils import table_rename


_column_adapter = {
    "Date": "date",
    "Location": "_location",
    "First Dose": "total_persons_vaccinated",
    "Second Dose": "total_persons_fully_vaccinated",
    "Total": "total_vaccine_doses_administered",
}


class FinMangoUkDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(dataframes[0], _column_adapter, drop=True)

        # Convert data to int type
        for col in data.columns[2:]:
            data[col] = data[col].apply(safe_int_cast)

        # Match data with GB subregions
        data["key"] = None
        data["country_code"] = "GB"
        data["subregion2_code"] = None
        data["locality_code"] = None

        # East Of England
        data.loc[data["_location"] == "Total", "key"] = "GB_ENG"
        data.loc[data["_location"] == "East Of England", "key"] = "GB_UKH"
        data.loc[data["_location"] == "London", "key"] = "GB_UKI"
        # data.loc[data["_location"] == "Midlands", "key"] = ""
        # data.loc[data["_location"] == "North East And Yorkshire", "key"] = ""
        data.loc[data["_location"] == "North West", "key"] = "GB_UKD"
        data.loc[data["_location"] == "South East", "key"] = "GB_UKJ"
        data.loc[data["_location"] == "South West", "key"] = "GB_UKK"

        return data
