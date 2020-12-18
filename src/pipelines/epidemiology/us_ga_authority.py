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
from lib.cached_data_source import CachedDataSource
from lib.utils import table_rename


class GeorgiaDeathsDataSource(CachedDataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        print(dataframes["US_GA_stratified"])
        raise ValueError()


class GeorgiaStratifiedDataSource(CachedDataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(
            dataframes["US_GA_stratified"],
            {
                "date": "date",
                "cases": "total_confirmed",
                "deaths": "total_deceased",
                "hospitalization": "total_hospitalized",
                "age group": "age",
                "county name": "match_string",
            },
            drop=True,
        )
        data["age"] = data.age.str.replace(" years", "")
        data.loc[data["age"] == "<1", "age"] = "00-04"
        data.loc[data["age"] == "01-04", "age"] = "00-04"
        data.loc[data["age"] == "80 & Older", "age"] = "80-"
        data.loc[data["age"] == "Unknown", "age"] = "age_unknown"

        data["country_code"] = "US"
        data["subregion1_code"] = "GA"

        data = data[~data["match_string"].str.contains("Unknown")]
        data.loc[data["match_string"] == "Georgia", "key"] = "US_GA"

        return data
