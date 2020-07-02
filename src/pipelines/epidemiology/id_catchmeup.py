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
from pandas import DataFrame, concat, merge
from lib.cast import safe_int_cast
from lib.pipeline import DataSource
from lib.time import datetime_isoformat
from lib.utils import grouped_diff, pivot_table


def _parse_date(date: str):
    return datetime_isoformat("%s-%d" % (date, datetime.datetime.now().year), "%d-%b-%Y")


class CatchmeupDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        df = dataframes[0]
        df.columns = df.iloc[0]
        df = df.rename(columns={"Provinsi": "date"})
        df = df.iloc[1:].set_index("date")

        df = df[df.columns.dropna()]
        df = pivot_table(df.transpose(), pivot_name="match_string")
        df["date"] = df["date"].apply(_parse_date)
        df = df.dropna(subset=["date"])
        df = df.rename(columns={"value": "confirmed"})
        df["confirmed"] = df["confirmed"].apply(safe_int_cast).astype("Int64")

        keep_columns = ["date", "match_string", "confirmed"]
        df = df[df["match_string"] != "Total"]
        df = df[df["match_string"] != "Dalam proses investigasi"]
        df = grouped_diff(df[keep_columns], ["match_string", "date"])
        df["country_code"] = "ID"
        return df
