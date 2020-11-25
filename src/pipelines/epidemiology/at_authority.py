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
from lib.utils import table_merge


class AustriaLevel2DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = table_merge(
            [
                dataframes["confirmed_deceased_recovered"].rename(
                    columns={
                        "AnzahlFaelle": "new_confirmed",
                        "AnzahlFaelleSum": "total_confirmed",
                        "AnzahlTotTaeglich": "new_deceased",
                        "AnzahlTotSum": "total_deceased",
                        "AnzahlGeheiltTaeglich": "new_recovered",
                        "AnzahlGeheiltSum": "total_recovered",
                    }
                ),
                dataframes["tested"].rename(
                    columns={
                        "TestGesamt": "total_tested",
                        "MeldeDatum": "Time"
                    }
                )
            ],
            how="outer",
        )

        # Convert date to ISO format
        data["date"] = data["Time"].apply(
            lambda x: datetime_isoformat(x, "%d.%m.%Y %H:%M:%S"))

        # Create the key from the state ID
        data["key"] = data["BundeslandID"].apply(lambda x: f"AT_{x}")

        data.loc[data["key"] == "AT_10", "key"] = "AT"

        # Output the results
        return data
