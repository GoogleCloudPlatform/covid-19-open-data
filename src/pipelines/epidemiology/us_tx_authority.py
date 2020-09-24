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
import numpy
from pandas import DataFrame
from lib.cast import safe_float_cast, safe_str_cast
from lib.io import read_file
from lib.data_source import DataSource
from lib.time import datetime_isoformat
from lib.utils import table_merge, table_rename


def _rename_columns(data: DataFrame, column_adapter: Dict[str, str]) -> DataFrame:
    error: Exception = None
    for header_index in (0, 1):
        try:
            df = data.copy()
            df.columns = df.iloc[header_index]
            df.columns = [str(col).replace("\n", " ") for col in df.columns]
            df = df.iloc[header_index + 1 :]
            df = df.replace(".", numpy.nan)
            df = table_rename(df, column_adapter)
            return df[column_adapter.values()].copy()
        except Exception as exc:
            error = exc
    raise error


class TexasDataSource(DataSource):
    @staticmethod
    def _parse_trends(data: DataFrame) -> DataFrame:
        column_adapter = {
            "Date": "date",
            "Cumulative Cases": "total_confirmed",
            "Cumulative Fatalities": "total_deceased",
            "Daily New Cases": "new_confirmed",
            "Fatalities by Date of Death": "new_deceased",
        }
        # The data source keeps switching formats, so we just try multiple options
        try:
            return _rename_columns(data, column_adapter)
        except:
            return _rename_columns(data.iloc[1:], column_adapter)

    @staticmethod
    def _parse_tests(data: DataFrame) -> DataFrame:
        return _rename_columns(
            data,
            {
                "Date": "date",
                "Molecular Tests": "total_tested",
                "Antibody Tests": "total_tested_antibody",
                "Total Tests reported": "total_tested_all",
            },
        )

    @staticmethod
    def _parse_hospitalized(data: DataFrame) -> DataFrame:
        return _rename_columns(data, {"Date": "date", "Hospitalizations": "current_hospitalized"})

    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        sheets = []
        sheet_processors = {
            "Trends": TexasDataSource._parse_trends,
            "Tests by day": TexasDataSource._parse_tests,
            "Hospitalization by Day": TexasDataSource._parse_hospitalized,
        }
        for sheet_name, sheet_processor in sheet_processors.items():
            df = sheet_processor(read_file(sources[0], sheet_name=sheet_name))
            df["date"] = df["date"].apply(safe_str_cast)
            df["date"] = df["date"].apply(lambda x: datetime_isoformat(x, "%Y-%m-%d %H:%M:%S"))
            df = df.dropna(subset=["date"])
            sheets.append(df)

        data = table_merge(sheets, how="outer")
        for col in data.columns:
            if col != "date":
                data[col] = data[col].apply(safe_float_cast).astype(float)

        data["key"] = "US_TX"
        return data
