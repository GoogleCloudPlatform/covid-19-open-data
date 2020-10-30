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


class SwitzerlandSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = (
            dataframes[0]
            .rename(
                columns={
                    "ncumul_tested": "total_tested",
                    "ncumul_conf": "total_confirmed",
                    "ncumul_deceased": "total_deceased",
                    "ncumul_hosp": "total_hospitalized",
                    "ncumul_ICU": "total_intensive_care",
                    "ncumul_vent": "total_ventilator",
                    "ncumul_released": "total_recovered",
                    "abbreviation_canton_and_fl": "subregion1_code",
                }
            )
            .drop(columns=["time", "source"])
        )

        # We can derive the key from country code + subregion1 code
        data["key"] = "CH_" + data["subregion1_code"]

        # Principality of Liechtenstein is not in CH
        data.loc[data["subregion1_code"] == "FL", "key"] = "LI"

        return data
