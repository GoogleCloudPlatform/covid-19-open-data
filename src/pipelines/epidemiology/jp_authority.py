# Copyright 2022 Google LLC
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
from pandas import DataFrame, concat, melt
from lib.data_source import DataSource
from lib.time import datetime_isoformat
from lib.utils import table_merge


class JapanDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_merge(
            [
                melt(dataframes[name], id_vars=["Date"], var_name="match_string", value_name=value)
                for name, value in [("confirmed", "new_confirmed"), ("deceased", "total_deceased")]
            ]
        )

        data["country_code"] = "JP"

        # Get date in ISO format
        data = data.rename(columns={"Date": "date"})
        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%Y/%m/%d"))

        # Country-level uses the label "ALL"
        country_mask = data["match_string"] == "ALL"
        country = data.loc[country_mask]
        data = data.loc[~country_mask]
        country["key"] = "JP"

        # Output the results
        return concat([country, data])
