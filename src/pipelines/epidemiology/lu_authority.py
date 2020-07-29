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


class LuxembourgDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        # The headers are a bit funny-looking, so we must manually manipulate them first
        data = dataframes[0]
        data.columns = [col.split("|")[0].split("~")[0] for col in data.iloc[0]]
        data = data.iloc[1:]

        data = table_rename(
            data,
            {
                "Date": "date",
                "Nombre de personnes en soins intensifs": "current_intensive_care",
                "Nombre cumulé de décès": "total_deceased",
                "Nombre de personnes testées COVID+": "new_tested",
            },
            drop=True,
        )

        # Get date in ISO format
        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%d/%m/%Y"))

        # Only country-level data is provided
        data["key"] = "LU"

        # Output the results
        return data
