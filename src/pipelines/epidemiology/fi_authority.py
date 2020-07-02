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

import json
import datetime
from typing import Dict, List
from pandas import DataFrame
from lib.pipeline import DataSource
from lib.utils import table_rename


_column_adapter = {
    "date": "date",
    "kunta": "subregion1_name",
    "tapauksia": "total_confirmed",
    "miehia": "total_confirmed_male",
    "naisia": "total_confirmed_female",
    "ika_0_9": "total_confirmed_age_00",
    "ika_10_19": "total_confirmed_age_01",
    "ika_20_29": "total_confirmed_age_02",
    "ika_30_39": "total_confirmed_age_03",
    "ika_40_49": "total_confirmed_age_04",
    "ika_50_59": "total_confirmed_age_05",
    "ika_60_69": "total_confirmed_age_06",
    "ika_70_79": "total_confirmed_age_07",
    "ika_80_": "total_confirmed_age_08",
}


# pylint: disable=missing-class-docstring,abstract-method
class FinlandArcGisDataSource(DataSource):
    def parse(self, sources: List[str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        data = json.load(open(sources[0]))["features"]
        data = table_rename(
            DataFrame.from_records([row["attributes"] for row in data]),
            _column_adapter,
            remove_regex=r"[^a-z\s\d]",
            drop=True,
        )

        # Add the age bins
        data["age_bin_00"] = "0-9"
        data["age_bin_01"] = "10-49"
        data["age_bin_02"] = "20-59"
        data["age_bin_03"] = "30-39"
        data["age_bin_04"] = "40-49"
        data["age_bin_05"] = "70-59"
        data["age_bin_06"] = "60-69"
        data["age_bin_07"] = "70-79"
        data["age_bin_08"] = "80-"

        # Convert date to ISO format
        data = data.dropna(subset=["date"])
        data.date = data.date.apply(
            lambda x: datetime.datetime.fromtimestamp(x // 1000).date().isoformat()
        )

        data["key"] = "FI"
        return data


class FinlandThlDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = table_rename(
            dataframes[0], {"Aika": "date", "Alue": "match_string", "val": "new_confirmed"}
        ).dropna(subset=["new_confirmed"])

        # Convert dates to ISO format
        data["date"] = data["date"].astype(str)

        data["country_code"] = "FI"
        return data
