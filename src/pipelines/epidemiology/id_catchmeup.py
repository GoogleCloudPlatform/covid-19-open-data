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
from typing import Dict
from pandas import DataFrame
from lib.cast import safe_int_cast
from lib.data_source import DataSource
from lib.time import datetime_isoformat
from lib.utils import pivot_table


def _parse_date(date: str):
    return datetime_isoformat("%s-%d" % (date, datetime.datetime.now().year), "%d-%b-%Y")


class CatchmeupDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        df = dataframes[0]
        df.columns = df.iloc[0]
        df = df.rename(columns={"Provinsi": "date"})
        df = df.iloc[1:].set_index("date")

        df = df[df.columns.dropna()]
        df = pivot_table(df.transpose(), pivot_name="match_string")
        df["date"] = df["date"].apply(_parse_date)
        df = df.dropna(subset=["date"])
        df = df.rename(columns={"value": "total_confirmed"})
        df["total_confirmed"] = df["total_confirmed"].apply(safe_int_cast).astype("Int64")

        df = df[["date", "match_string", "total_confirmed"]]
        df = df[df["match_string"] != "Total"]
        df = df[df["match_string"] != "Dalam proses investigasi"]
        df["country_code"] = "ID"
        return df
