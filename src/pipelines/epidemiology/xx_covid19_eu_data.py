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

from datetime import datetime
from typing import Any, Dict, List
from pandas import DataFrame, concat, merge
from lib.pipeline import DataSource
from lib.utils import grouped_diff


class Covid19EuDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = dataframes[0].rename(
            columns={
                "datetime": "date",
                "country": "country_code",
                "nuts_2": "match_string",
                "nuts_3": "match_string",
                "cases": "confirmed",
                "deaths": "deceased",
                "tests": "tested",
                "recovered": "recovered",
                "hospitalized": "hospitalized",
                "intensive_care": "icu",
            }
        )
        data["date"] = data["date"].apply(lambda x: datetime.fromisoformat(x))
        data["date"] = data["date"].apply(lambda x: x.date().isoformat())

        # Remove bogus data
        blacklist = ("unknown", "unknown county", "nezjištěno", "outside mainland norway")
        data = data.dropna(subset=["match_string"])
        data.match_string = data.match_string.str.lower()
        data = data[~data["match_string"].isin(blacklist)]
        data = data[~data["match_string"].apply(lambda x: len(x) == 0 or x.startswith("http"))]

        # Remove unnecessary columns
        data = data[[col for col in data.columns if not "/" in col]]

        # Some tables have repeated data
        data = data.groupby(["country_code", "match_string", "date"]).last().reset_index()

        return grouped_diff(
            data,
            ["country_code", "match_string", "date"],
            skip=["tests", "recovered", "hospitalized", "icu"],
        )
