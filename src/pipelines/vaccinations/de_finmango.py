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
from pandas import DataFrame, concat
from lib.cast import safe_int_cast
from lib.data_source import DataSource
from lib.utils import table_rename
from pipelines.epidemiology.de_authority import _SUBREGION1_CODE_MAP


_column_adapter = {
    "Date": "date",
    "RS": "subregion1_code",
    "New number vaccinated at least once": "new_persons_vaccinated",
    "New Fully Vaccinated": "new_persons_fully_vaccinated",
    "New Vaccinations": "new_vaccine_doses_administered",
    "Total number vaccinated at least once": "total_persons_vaccinated",
    "Total Fully Vaccinated": "total_persons_fully_vaccinated",
    "Total Vaccinations": "total_vaccine_doses_administered",
    "Total First Dose BioNTech": "new_persons_vaccinated_pfizer",
    "Total Second Dose BioNTech": "total_persons_fully_vaccinated_pfizer",
    "Total First Dose Moderna": "new_persons_vaccinated_moderna",
    "Total Second Dose Moderna": "total_persons_fully_vaccinated_moderna",
    "Total First Dose AstraZeneca": "new_persons_vaccinated_astrazeneca",
    "Total Second Dose AstraZeneca": "total_persons_fully_vaccinated_astrazeneca",
    "Total First Dose Janssen": "new_persons_vaccinated_janssen",
    "Total Second Dose Janssen": "total_persons_fully_vaccinated_janssen",
}


class FinMangoGermanyDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(concat(dataframes[0].values()), _column_adapter, drop=True)

        # Remove records with no date or location
        data = data.dropna(subset=["date", "subregion1_code"])

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
