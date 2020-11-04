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
from lib.cast import safe_int_cast, numeric_code_as_string
from lib.data_source import DataSource

_columns = [
    "date",
    "subregion1_name",
    "subregion1_code",
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
    "source",
]


class FinMangoDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        # Data is nested into multiple sheets
        tables = []
        for df in list(dataframes[0].values())[1:]:
            # Header has two rows, but we ignore them and use our own columns anyway
            df.columns = _columns
            df = df.iloc[2:].copy()

            # Make sure subregion code is numeric
            apply_func = lambda x: numeric_code_as_string(x, 2)
            df["subregion1_code"] = df["subregion1_code"].apply(apply_func)

            # Keep only new_confirmed
            df = df[["date", "subregion1_code"] + parse_opts["columns"]]

            # Keep only rows with indexable columns not null
            df.dropna(subset=["date", "subregion1_code"], inplace=True)

            # This data source is "complete" so all nulls are zeroes
            df = df.fillna(0)

            # Add to the tables including all subregions
            tables.append(df.iloc[1:])

        # Put all sheets together into a single DataFrame
        data = concat(tables)

        # Derive the key from country and region code
        data["key"] = parse_opts["country"] + "_" + data["subregion1_code"]
        data.drop(columns=["subregion1_code"], inplace=True)

        # Ensure date is in ISO format
        data["date"] = data["date"].apply(lambda x: str(x)[:10])

        # Make sure that all data is numeric
        for col in data.columns:
            if col not in ("date", "key"):
                data[col] = data[col].apply(safe_int_cast)

        # Output the results
        return data
