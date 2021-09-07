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
from lib.data_source import DataSource
from lib.utils import table_rename


class PakistanCountryDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(
            dataframes[0],
            {
                "date": "date",
                "confirmed_cases": "new_confirmed",
                "deaths": "new_deceased",
                "tests_performed": "new_tested",
            },
            drop=True,
        )
        # Ensure date is str type
        data["date"] = data["date"].astype(str)

        data["key"] = "PK"
        return data
