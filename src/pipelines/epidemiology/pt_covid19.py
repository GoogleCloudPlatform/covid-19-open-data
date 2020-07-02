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

import datetime
from typing import Any, Dict, List
from numpy import unique
from pandas import DataFrame, concat, merge
from lib.pipeline import DataSource
from lib.time import datetime_isoformat
from lib.utils import grouped_diff, pivot_table


class PtCovid19L1DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = dataframes[0]
        rename_columns = {
            "date": "date",
            "cases_confirmed": "total_confirmed",
            "cases_confirmed_new": "new_confirmed",
            "deaths": "total_deceased",
            "deaths_new": "new_deceased",
            "recovered": "total_recovered",
            "recovered_new": "new_recovered",
            "inpatient": "current_hospitalized",
            "icu": "current_intensive_care",
        }
        data = data.rename(columns=rename_columns)[list(rename_columns.values())]
        data.date = data.date.apply(lambda x: datetime_isoformat(x, "%d-%m-%Y"))
        data["key"] = "PT"
        return data


class PtCovid19L2DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = dataframes[0]
        column_tokens = ["confirmed_", "deaths_", "recovered_"]
        data = data[[col for col in data.columns if any(token in col for token in column_tokens)]]
        data = data.drop(
            columns=["cases_confirmed_new", "cases_unconfirmed_new", "deaths_new", "recovered_new"]
        )
        data["date"] = dataframes[0].date.apply(lambda x: datetime_isoformat(x, "%d-%m-%Y"))

        subsets = []
        for token in column_tokens:
            df = data[["date"] + [col for col in data.columns if token in col]]
            df = pivot_table(df.set_index("date"), pivot_name="match_string")
            df.match_string = df.match_string.apply(lambda x: x.split("_", 2)[1])
            df = df.rename(columns={"value": token.split("_")[0]})
            subsets.append(df)

        data = subsets[0]
        for df in subsets[1:]:
            data = data.merge(df, how="outer")
        data = data.rename(columns={"deaths": "deceased"})

        data = data[data.match_string != "unconfirmed"]
        data = grouped_diff(data, ["match_string", "date"])
        data["country_code"] = "PT"
        return data
