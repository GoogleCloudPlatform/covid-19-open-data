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
from lib.pipeline import DataSource
from lib.time import datetime_isoformat
from lib.utils import table_rename


class IndianaDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = table_rename(
            dataframes[0],
            {
                "DATE": "date",
                "COUNTY_NAME": "match_string",
                "COVID_COUNT": "new_confirmed",
                "agegrp": "age",
                "gender": "sex",
            },
        )

        data.sex = data.sex.apply(lambda x: x.replace("M", "male").replace("F", "female"))
        data.date = data.date.apply(lambda x: datetime_isoformat(x, "%Y-%m-%d %H:%M:%S"))
        data.age = data.age.apply(lambda x: None if x == "Unknown" else x.replace("+", "-"))

        return data
