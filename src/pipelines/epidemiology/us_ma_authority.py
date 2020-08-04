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
from pandas import DataFrame
from lib.data_source import DataSource
from lib.time import datetime_isoformat
from lib.utils import table_rename


class MassachusettsCountiesDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(
            dataframes["counties"],
            {
                "Date": "date",
                "County": "match_string",
                "Count": "total_confirmed",
                "Deaths": "total_deceased",
            },
        )

        # Convert date to ISO format
        data["date"] = data["date"].astype(str).apply(lambda x: datetime_isoformat(x, "%m/%d/%Y"))

        # Drop bogus values
        data = data[data["match_string"] != "Unknown"]

        # Dukes and Nantucket are separate counties but reported as one, so drop them from the data
        data = data[data["match_string"] != "Dukes and Nantucket"]

        data["country_code"] = "US"
        data["subregion1_code"] = "MA"
        return data


class MassachusettsBySexDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(
            dataframes["by-sex"],
            {
                "Date": "date",
                "Male": "total_confirmed_male",
                "Female": "total_confirmed_female",
                "Unknown": "total_confirmed_sex_unknown",
            },
        )

        # Convert date to ISO format
        data["date"] = data["date"].astype(str).apply(lambda x: datetime_isoformat(x, "%m/%d/%Y"))

        data["key"] = "US_MA"
        return data


class MassachusettsByAgeDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(
            dataframes["by-age"],
            {
                "Date": "date",
                "Age": "age",
                "Cases": "total_confirmed",
                "Hospitalized": "total_hospitalized",
                "Deaths": "total_deceased",
            },
        )

        # Format the age buckets
        data["age"] = data["age"].apply(lambda x: None if x == "Unknown" else x.replace("+", "-"))

        # Convert date to ISO format
        data["date"] = data["date"].astype(str).apply(lambda x: datetime_isoformat(x, "%m/%d/%Y"))

        data["key"] = "US_MA"
        return data
