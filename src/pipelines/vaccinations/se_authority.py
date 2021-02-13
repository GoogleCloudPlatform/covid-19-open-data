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

import datetime
from typing import Any, Dict
from pandas import DataFrame, concat
from lib.data_source import DataSource
from lib.time import datetime_isoformat
from lib.utils import aggregate_admin_level, table_merge, table_rename
from pipelines.epidemiology.it_authority import _subregion1_code_converter


_column_adapter = {
    "Vecka": "week",
    "År": "year",
    "Region": "match_string",
    "Antal vaccinerade": "_total_doses",
    # "Andel vaccinerade": "",
    "Dosnummer": "_dose_type",
}


class SwedenDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(dataframes[0], _column_adapter, drop=True)

        # Convert date to ISO format
        data["date"] = data["year"].apply(lambda x: datetime.datetime.strptime(str(x), "%Y"))
        data["date"] = data["date"] + data["week"].apply(lambda x: datetime.timedelta(weeks=x))
        data["date"] = data["date"].apply(lambda x: x.date().isoformat())
        data = data.drop(columns=["week", "year"])

        # Process 1-dose and 2-dose separately
        data_1_dose = data[data["_dose_type"].str.slice(-1) == "1"].drop(columns=["_dose_type"])
        data_2_dose = data[data["_dose_type"].str.slice(-1) == "2"].drop(columns=["_dose_type"])
        data_1_dose = data_1_dose.rename(columns={"_total_doses": "total_persons_vaccinated"})
        data_2_dose = data_2_dose.rename(columns={"_total_doses": "total_persons_fully_vaccinated"})
        data = table_merge([data_1_dose, data_2_dose], how="outer")

        # Make sure only subregion1 matches
        data["key"] = None
        data["country_code"] = "SE"
        data["subregion2_code"] = None
        data["locality_code"] = None

        # Country totals are reported using a special name
        data.loc[data["match_string"] == "| Sverige |", "key"] = "SE"

        # Estimate the total doses from person counts
        data["total_vaccine_doses_administered"] = (
            data["total_persons_vaccinated"] + data["total_persons_fully_vaccinated"]
        )

        return data
