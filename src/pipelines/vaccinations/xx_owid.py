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
from lib.cast import safe_int_cast
from lib.data_source import DataSource
from lib.time import date_offset
from lib.utils import get_or_default, table_rename


_column_adapter = {
    "location": "match_string",
    "iso_code": "3166-1-alpha-3",
    "date": "date",
    "total_vaccinations": "total_vaccine_doses_administered",
    "people_vaccinated": "total_persons_vaccinated",
    "people_fully_vaccinated": "total_persons_fully_vaccinated",
    # daily_vaccinations_raw,
    # daily_vaccinations,
    # total_vaccinations_per_hundred,
    # people_vaccinated_per_hundred,
    # people_fully_vaccinated_per_hundred,
    # daily_vaccinations_per_million,
}


class OurWorldInDataSource(DataSource):
    @staticmethod
    def _adjust_date(data: DataFrame, metadata: DataFrame) -> DataFrame:
        """ Adjust the date of the data based on the report offset """

        # Save the current columns to filter others out at the end
        data_columns = data.columns

        # Filter auxiliary dataset to only get the relevant keys
        data = data.merge(metadata, suffixes=("", "aux_"), how="left")

        # Perform date adjustment for all records so date is consistent across datasets
        data.aggregate_report_offset = data.aggregate_report_offset.apply(safe_int_cast)
        data["date"] = data.apply(
            lambda x: date_offset(x["date"], get_or_default(x, "aggregate_report_offset", 0)),
            axis=1,
        )

        return data[data_columns]

    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(dataframes[0], _column_adapter, drop=True).merge(aux["country_codes"])

        # Adjust the date of the records to match local reporting
        data = self._adjust_date(data, aux["metadata"])

        return data


class OurWorldInDataUSDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(dataframes[0], _column_adapter, drop=True)

        # Match data with US states
        data["key"] = None
        data["country_code"] = "US"
        data["subregion2_code"] = None
        data["locality_code"] = None

        # Fix mismatching names
        data.loc[data["match_string"] == "New York State", "match_string"] = "New York"

        # Some nations are reported as states
        data.loc[data["match_string"] == "Marshall Islands", "key"] = "MH"
        data.loc[data["match_string"] == "Republic of Palau", "key"] = "PW"

        return data
