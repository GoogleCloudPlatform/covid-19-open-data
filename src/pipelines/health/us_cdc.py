# Copyright 2021 Google LLC
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
from typing import Dict
from pandas import DataFrame
from lib.data_source import DataSource
from lib.utils import table_rename


class CDCDataSource(DataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        dataframes = {}
        for name, file_path in sources.items():
            with open(file_path, "r") as fd:
                records = json.load(fd)["data"]
                dataframes[name] = DataFrame.from_records(records)
        return self.parse_dataframes(dataframes, aux, **parse_opts)

    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(dataframes[0], {"STATE": "state", "RATE": "life_expectancy"}, drop=True)
        data["key"] = "US_" + data["state"]
        return data
