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
from bs4 import BeautifulSoup
from pandas import DataFrame
from lib.pipeline import DataSource
from lib.time import datetime_isoformat


class CzechRepublicL1HospitalizedDataSource(DataSource):
    def parse(self, sources: List[str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        # Get the file contents from source
        html_content = open(sources[0]).read()
        page = BeautifulSoup(html_content, "lxml")
        data = page.select("#panel3-hospitalization")[0]
        data = data.select("#js-hospitalization-table-data")[0]
        data = json.loads(data.attrs.get("data-table", '{"body":[]}'))["body"]
        data = DataFrame.from_records(
            [
                {
                    "date": datetime_isoformat(row[0], "%d.%m.%Y"),
                    "current_hospitalized": row[1],
                    "current_intensive_care": row[2],
                }
                for row in data
            ]
        )
        data["key"] = "CZ"
        return data
