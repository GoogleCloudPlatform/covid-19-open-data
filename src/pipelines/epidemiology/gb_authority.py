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

from pathlib import Path
from typing import Any, Dict, List
from pandas import DataFrame, isnull, merge
from lib.pipeline import DataSource
from lib.utils import table_rename
from lib.time import datetime_isoformat
from lib.cast import safe_int_cast
import requests


class Covid19UkL2DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = table_rename(
            dataframes[0],
            {
                "areaName": "subregion1_name",
                "cumCasesByPublishDate": "total_confirmed",
                "cumDeathsByPublishDate": "total_deceased",
                "cumPillarOneTestsByPublishDate": "total_tested",
                "date": "date",
            },
            drop=True,
        )

        data = data[data["subregion1_name"] != "UK"]
        data.date = data.date.apply(lambda x: datetime_isoformat(x, "%Y-%m-%d"))

        # Bug in data at time of writing.
        data = data[data["date"] != "2020-12-07"]

        # Make sure all records have country code and no subregion code
        data["country_code"] = "GB"
        data["subregion2_code"] = None

        return data


class Covid19UkL1DataSource(Covid19UkL2DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = super().parse_dataframes(dataframes, aux, **parse_opts)

        # Aggregate data to the country level
        data = data.groupby("date").sum().reset_index()
        data["key"] = "GB"
        return data
