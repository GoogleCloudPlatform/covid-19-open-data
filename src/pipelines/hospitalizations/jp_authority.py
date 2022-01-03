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
        column_adapter = {
            "Requiring inpatient care": "new_hospitalized",
            "Discharged from hospital or released from treatment": "new_recovered",
        }

        tables = []
        for col_prev, col_value in column_adapter.items():
            keep_cols = [col for col in dataframes[0].columns if col_prev in col]
            df = dataframes[0][["Date"] + keep_cols]
            df = melt(df, id_vars=["Date"], var_name="match_string", value_name=col_value)
            df["match_string"] = df["match_string"].apply(lambda x: x.split(" ")[0][1:-1])
            tables.append(df)

        data = table_merge(tables)
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
