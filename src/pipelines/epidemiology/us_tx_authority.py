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

import sys
from typing import Any, Dict, List
import numpy
from pandas import DataFrame, concat, merge
from lib.cast import safe_float_cast
from lib.io import read_file
from lib.pipeline import DataSource
from lib.time import datetime_isoformat
from lib.utils import table_multimerge, table_rename


class TexasDataSource(DataSource):
    @staticmethod
    def _rename_columns(data: DataFrame, column_adapter: Dict[str, str]) -> DataFrame:
        data.columns = data.iloc[0]
        data.columns = [str(col).replace("\n", " ") for col in data.columns]
        data = table_rename(data.iloc[1:].replace(".", numpy.nan), column_adapter)
        return data[column_adapter.values()]

    @staticmethod
    def _parse_trends(data: DataFrame) -> DataFrame:
        return TexasDataSource._rename_columns(
            data.iloc[1:],
            {
                "Date": "date",
                "Cumulative Cases": "total_confirmed",
                "Cumulative Fatalities": "total_deceased",
                "Daily New Cases": "new_confirmed",
                "Daily New Fatalities": "new_deceased",
            },
        )

    @staticmethod
    def _parse_tests(data: DataFrame) -> DataFrame:
        return TexasDataSource._rename_columns(
            data,
            {
                "Date": "date",
                "Viral Tests": "total_tested",
                "Antibody Tests": "total_tested_antibody",
                "Total Tests reported": "total_tested_all",
            },
        )

    @staticmethod
    def _parse_hospitalized(data: DataFrame) -> DataFrame:
        return TexasDataSource._rename_columns(
            data, {"Date": "date", "Hospitalizations": "current_hospitalized"}
        )

    def parse(self, sources: List[str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        sheets = []
        sheet_processors = {
            "Trends": TexasDataSource._parse_trends,
            "Tests by day": TexasDataSource._parse_tests,
            "Hospitalization by Day": TexasDataSource._parse_hospitalized,
        }
        for sheet_name, sheet_processor in sheet_processors.items():
            df = sheet_processor(read_file(sources[0], sheet_name=sheet_name))
            df = df.dropna(subset=["date"])
            df.date = df.date.astype(str)
            df.date = df.date.apply(lambda x: datetime_isoformat(x, "%Y-%m-%d %H:%M:%S"))
            sheets.append(df)

        data = table_multimerge(sheets, how="outer")
        for col in data.columns:
            if col != "date":
                data[col] = data[col].apply(safe_float_cast).astype(float)

        data["key"] = "US_TX"
        return data
