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
from pipelines.epidemiology.de_authority import _SUBREGION1_CODE_MAP


_column_adapter = {
    "Date": "date",
    "RS": "subregion1_code",
    "Total First Dose": "total_persons_vaccinated",
    "Total Second Dose": "total_persons_fully_vaccinated",
    "Total Vaccinations": "total_vaccine_doses_administered",
}


class FinMangoGermanyDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(dataframes[0], _column_adapter, drop=True)

        # Convert data to int type
        for col in data.columns[2:]:
            data[col] = data[col].apply(safe_int_cast)

        # Use proper ISO codes for the subregion1 level
        data["subregion1_code"] = data["subregion1_code"].apply(_SUBREGION1_CODE_MAP.get)

        # Blank region code is used for country-level data
        data["key"] = None
        data.loc[data["subregion1_code"].isna(), "key"] = "DE"
        data.loc[data["subregion1_code"].notna(), "key"] = "DE_" + data["subregion1_code"]

        return data
