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
    "areaType": "_level",
    "areaName": "subregion1_name",
    "cumPeopleVaccinatedFirstDoseByPublishDate": "total_persons_vaccinated",
    "cumPeopleVaccinatedSecondDoseByPublishDate": "total_persons_fully_vaccinated",
}


class GreatBritainDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(dataframes[0], _column_adapter, drop=True)

        # Compute total doses from persons vaccinated
        data["total_vaccine_doses_administered"] = (
            data["total_persons_vaccinated"] + data["total_persons_fully_vaccinated"]
        )

        if data.iloc[0]["_level"] == "overview":
            data["key"] = "GB"

        # Match data with GB subregions
        data["country_code"] = "GB"
        data["subregion2_code"] = None
        data["locality_code"] = None

        return data
