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
import math
from pandas import DataFrame, melt
from lib.data_source import DataSource
from lib.time import datetime_isoformat
import datetime


class Covid19IndiaOrgDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = dataframes[0]
        # Get all the states
        states = list(data.columns.difference(["Status", "Date"]))
        # Flatten the table
        data = melt(data, id_vars=["Date", "Status"], value_vars=states, var_name="subregion1_code")
        # Pivot on Status to get flattened confirmed, deceased, recovered numbers
        data = data.pivot_table("value", ["Date", "subregion1_code"], "Status")
        data.reset_index(drop=False, inplace=True)
        data = data.reindex(
            ["Date", "subregion1_code", "Confirmed", "Deceased", "Recovered"], axis=1
        )

        data = data.rename(
            columns={
                "Confirmed": "new_confirmed",
                "Deaths": "new_deceased",
                "Recovered": "new_recovered",
                "Date": "date",
            }
        )

        data.date = data.date.apply(lambda x: datetime_isoformat(x, "%d-%b-%y"))

        data["country_code"] = "IN"

        return data
