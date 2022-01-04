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
from lib.time import date_today

_columns = [
    "date",
    "subregion2_name",
    "new_confirmed",
    "total_confirmed",
    "new_deceased",
    "total_deceased",
    "new_tested",
]


class FinMangoDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        # Data is nested into multiple sheets
        tables = []
        for df in list(dataframes[0].values()):
            # Header has two rows, but we ignore them and use our own columns anyway
            df.columns = _columns
            df = df.iloc[2:].copy()

            # Keep only rows with indexable columns not null
            df.dropna(subset=["date", "subregion2_name"], inplace=True)

            # Add to the tables including all subregions
            tables.append(df.iloc[1:])

        # Put all sheets together into a single DataFrame
        data = concat(tables)

        # Ensure date is in ISO format
        data["date"] = data["date"].apply(lambda x: str(x)[:10])

        # Make sure that all data is numeric
        for col in data.columns:
            if col not in ("date", "subregion2_name"):
                data[col] = data[col].apply(safe_int_cast)

        # Filter out dates beyond today
        data = data[data["date"] < date_today(offset=1)]

        # Output the results
        data["country_code"] = "SL"
        return data
