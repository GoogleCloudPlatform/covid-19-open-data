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
from pandas import DataFrame, concat
from lib.io import fuzzy_text
from lib.pipeline import DataSource
from lib.time import datetime_isoformat
from lib.utils import pivot_table_date_columns


class BangladeshHumdataDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        tables = []
        dataframes[0].columns = [
            fuzzy_text(col, remove_regex=r"[^0-9a-z\s_]", remove_spaces=False)
            for col in dataframes[0].columns
        ]
        for keyword, value_column in [
            ("confirmed", "total_confirmed"),
            ("death", "total_deceased"),
            ("recover", "total_recovered"),
        ]:
            data = dataframes[0][
                ["_name"] + [col for col in dataframes[0].columns if keyword in col]
            ]
            data.columns = [col.split(" upto ", 2)[-1] for col in data.columns]
            data = data.set_index("_name")[data.columns[1:]]
            data = pivot_table_date_columns(data, pivot_name="date", value_name=value_column)
            data.date = data.date.apply(lambda x: datetime_isoformat(f"{x}-2020", "%d %B-%Y"))
            data = data.reset_index().rename(columns={"index": "match_string"})
            tables.append(data)

        # Aggregate all tables together
        data = concat(tables)

        # Make sure all records have the country code
        data["country_code"] = "BD"

        # Output the results
        return data
