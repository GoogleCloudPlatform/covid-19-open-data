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
from typing import Dict, List
from pandas import DataFrame
from lib.pipeline import DataSource
from lib.utils import table_rename, table_multimerge


class Covid19RomaniaDataSource(DataSource):
    def parse(self, sources: List[str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        data_list = []
        for idx, var in enumerate(["total_recovered", "current_intensive_care"]):
            df = DataFrame.from_records(json.load(open(sources[idx]))["values"])
            data_list.append(table_rename(df, {"value": var}))

        data = table_multimerge(data_list)
        data["key"] = "RO"
        return data
