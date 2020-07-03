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
from lib.data_source import DataSource
from lib.utils import pivot_table


class Mexicovid19DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = None
        ordered_columns = [
            "new_confirmed",
            "new_deceased",
            "new_tested",
            "new_hospitalized",
            "new_intensive_care",
        ]
        for column_name, df in zip(ordered_columns, dataframes):
            df = df.rename(columns={"Fecha": "date"}).set_index("date")
            df = pivot_table(df, pivot_name="match_string").rename(columns={"value": column_name})
            if data is None:
                data = df
            else:
                data = data.merge(df, how="left")

        # Country-level have a specific label
        data["country_code"] = "MX"
        data.loc[data.match_string == "Nacional", "key"] = "MX"

        return data
