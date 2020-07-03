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
from lib.io import read_file
from lib.pipeline import DataSource
from lib.time import datetime_isoformat
from lib.utils import table_rename

_column_adapter = {
    "Date Reported": "date",
    "Date Completed": "date",
    "All Cases": "new_confirmed",
    "All Cases Cumulative": "total_confirmed",
    "Deceased Cases": "new_deceased",
    "Deceased Cases Cumulative": "total_deceased",
    "Hospitalized Cases": "new_hospitalized",
    "Hospitalized Cases Cumulative": "total_hospitalized",
    "All Tests": "new_tested",
    "All Tests Cumulative": "total_tested",
}


def _parse_statewide(data: DataFrame) -> DataFrame:
    data.columns = data.iloc[1]
    return table_rename(data.iloc[2:], _column_adapter, drop=True)


def _parse_testing(data: DataFrame) -> DataFrame:
    data.columns = data.iloc[1]
    return table_rename(data.iloc[2:], _column_adapter, drop=True)


_sheet_processors = {"Table 1": _parse_statewide, "Table 4": _parse_testing}


class AlaskaDataSource(DataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        data = read_file(sources[0], sheet_name=parse_opts.get("sheet_name"))
        data.columns = data.iloc[1]
        data = table_rename(data.iloc[2:], _column_adapter, drop=True)
        data.date = data.date.astype(str).apply(lambda x: x[:10])
        data.date = data.date.apply(lambda x: datetime_isoformat(x, "%Y-%m-%d"))
        data = data.dropna(subset=["date"])

        if parse_opts.get("key"):
            data["key"] = parse_opts.get("key")

        return data
