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

from typing import Any, Dict
from pandas import DataFrame
from lib.pipeline import DataSource
from lib.utils import table_rename

_column_adapter = {
    "Time": "date",
    "Variant": "indicator",
    "Location": "country_name",
    "PopTotal": "population",
    "PopMale": "population_male",
    "PopFemale": "population_female",
    "PopDensity": "population_density",
    "AgeGrpStart": "age",
}

_dummy_record = {
    "population_age_00_09": 0,
    "population_age_10_19": 0,
    "population_age_20_29": 0,
    "population_age_30_39": 0,
    "population_age_40_49": 0,
    "population_age_50_59": 0,
    "population_age_60_69": 0,
    "population_age_70_79": 0,
    "population_age_80_and_older": 0,
}


class UnWppByAgeDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = table_rename(dataframes[0], _column_adapter, drop=True,)

        # Filter data for 2020 and remove all other years
        data = data[data["date"] == 2020].drop(columns=["date"])

        # We only care about the population count indicators
        data = data[data["indicator"] == "Medium"]

        # Population counts are in thousands, convert back to single units
        for col in [col for col in data.columns if col.startswith("population")]:
            data[col] = data[col] * 1000

        # Derive key from our country names mapping
        names = aux["un_country_names"]
        data = data.merge(names, how="left")

        # Combine the age groups into one record per location
        records: Dict[str, Any] = {}
        for key, row in data.set_index("key").iterrows():
            records[key] = records.get(key, dict(_dummy_record, key=key))
            if row["age"] >= 80:
                records[key]["population_age_80_and_older"] += row["population"]
            else:
                for i in range(0, 8):
                    if row["age"] < 10 * (i + 1):
                        records[key][f"population_age_{i}0_{i}9"] += row["population"]
                        break

        return DataFrame.from_records(list(records.values()))


class UnWppBySexDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = table_rename(dataframes[0], _column_adapter, drop=True,)

        # Filter data for 2020 and remove all other years
        data = data[data["date"] == 2020].drop(columns=["date"])

        # We only care about the population count indicators
        data = data[data["indicator"] == "Medium"]

        # Population counts are in thousands, convert back to single units
        for col in [col for col in data.columns if col.startswith("population")]:
            data[col] = data[col] * 1000

        # Derive key from our country names mapping
        names = aux["un_country_names"]
        data = data.merge(names, how="left")

        return data
