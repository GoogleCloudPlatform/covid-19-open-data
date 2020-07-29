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
from pandas import DataFrame, concat
from lib.cast import safe_int_cast
from lib.data_source import DataSource
from lib.utils import table_rename

_columns = [
    "new_confirmed",
    "current_confirmed",
    "total_confirmed",
    "new_deceased",
    "current_deceased",
    "total_deceased",
    "new_tested",
    "current_tested",
    "total_tested",
    "new_hospitalized",
    "current_hospitalized",
    "total_hospitalized",
    "new_intensive_care",
    "current_intensive_care",
    "total_intensive_care",
    "new_ventilator",
    "current_ventilator",
    "total_ventilator",
]


class FinMangoDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        # Data is nested into multiple sheets
        tables = []
        for df in dataframes[0].values():
            # Header has two rows
            df.columns = df.iloc[0]

            # Many column names are repeated, so we disambiguate them
            index_columns = ["date", "name"]
            repeat_columns = df.columns[len(index_columns) :]
            df.columns = index_columns + [f"{col}_{idx}" for idx, col in enumerate(repeat_columns)]

            tables.append(df.iloc[1:])

        # Put all sheets together into a single DataFrame
        data = concat(tables)

        data = table_rename(data, {"Date": "date", "Code": "key"}).dropna(subset=["date", "key"])
        data["key"] = parse_opts["country"] + "_" + data["key"].str.replace("-", "_")

        # Ensure date is in ISO format
        data["date"] = data["date"].apply(lambda x: str(x)[:10])

        # The column names are always in the same order
        current_columns = data.columns.values
        replace_columns = ["date", "name", "key"] + _columns
        for idx, col in enumerate(replace_columns):
            current_columns[idx] = col
        data.columns = current_columns
        data = data[replace_columns]

        # Make sure that all data is numeric
        for col in data.columns:
            if col not in ("date", "name", "key"):
                data[col] = data[col].apply(safe_int_cast)

        # Remove the "new" columns since cumulative data is more reliable
        data = data.drop(columns=["new_confirmed", "new_deceased"])

        # Output the results
        return data
