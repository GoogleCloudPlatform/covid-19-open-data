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
from lib.time import datetime_isoformat
from lib.utils import table_multimerge, table_rename

_column_adapter = {
    "province": "subregion1_name",
    "health_region": "match_string",
    "date_report": "date",
    "date_death_report": "date",
    "cases": "new_confirmed",
    "deaths": "new_deceased",
    "cumulative_cases": "total_confirmed",
    "cumulative_deaths": "total_deceased",
}

_province_map = {
    "Alberta": "AB",
    "BC": "BC",
    "Manitoba": "MB",
    "NL": "NL",
    "NWT": "NT",
    "New Brunswick": "NB",
    "Nova Scotia": "NS",
    "Ontario": "ON",
    "PEI": "PE",
    "Quebec": "QC",
    "Saskatchewan": "SK",
    "Yukon": "YT",
}


class Covid19CanadaDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = table_multimerge(
            [
                table_rename(dataframes["confirmed"], _column_adapter, drop=True),
                table_rename(dataframes["deceased"], _column_adapter, drop=True),
            ],
            how="outer",
        )

        # Province names are sometimes codes (but not always compliant with ISO codes)
        data["subregion1_code"] = data["subregion1_name"].apply(_province_map.get)
        data.drop(columns=["subregion1_name"], inplace=True)

        # Convert date to ISO format
        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%d-%m-%Y"))

        # Aggregate subregion1 level
        l1_index = ["date", "subregion1_code"]
        l1 = data.drop(columns=["match_string"]).groupby(l1_index).sum().reset_index()

        # Make sure all records have the country code and subregion2_name
        l1["country_code"] = "CA"
        l1["subregion2_name"] = None
        data["country_code"] = "CA"
        data["subregion2_name"] = ""

        # Remove bogus data
        data = data[data["match_string"] != "Not Reported"]

        # Output the results
        return concat([l1, data])
