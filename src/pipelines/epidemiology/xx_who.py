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

from typing import Dict, List
from pandas import DataFrame
from lib.cast import safe_int_cast
from lib.data_source import DataSource
from lib.time import date_offset
from lib.utils import get_or_default, table_rename


def _adjust_date(data: DataFrame, aux: DataFrame) -> DataFrame:
    """ Adjust the date of the data based on the report offset """

    # Save the current columns to filter others out at the end
    data_columns = data.columns

    # Filter auxiliary dataset to only get the relevant keys
    data = data.merge(aux, suffixes=("", "aux_"), how="left")

    # Perform date adjustment for all records so date is consistent across datasets
    data.aggregate_report_offset = data.aggregate_report_offset.apply(safe_int_cast)
    data["date"] = data.apply(
        lambda x: date_offset(x["date"], get_or_default(x, "aggregate_report_offset", 0)), axis=1
    )

    return data[data_columns]


class WHODataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        # Date_reported	 Country_code	 Country	 WHO_region	 New_cases	 Cumulative_cases	 New_deaths	 Cumulative_deaths
        data = table_rename(
            dataframes[0],
            {
                "Date_reported": "date",
                "Country_code": "key",
                "New_cases": "new_confirmed",
                "Cumulative_cases": "total_confirmed",
                "New_deaths": "new_deceased",
                "Cumulative_deaths": "total_deceases",
            },
            drop=True,
        )

        # Convert date to ISO format
        data["date"] = data["date"].astype(str).apply(lambda x: x[:10])

        # Adjust the date of the records to match local reporting
        data = _adjust_date(data, aux["metadata"])

        # Remove bogus entries
        data = data[data["key"].str.strip() != ""]

        # We consider some countries as subregions of other countries
        data.loc[data["key"] == "BL", "key"] = "FR_BL"
        data.loc[data["key"] == "GP", "key"] = "FR_GUA"
        data.loc[data["key"] == "MF", "key"] = "FR_MF"
        data.loc[data["key"] == "PM", "key"] = "FR_PM"

        return data
