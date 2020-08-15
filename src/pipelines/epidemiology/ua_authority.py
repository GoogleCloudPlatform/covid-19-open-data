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
from typing import Dict

import requests
from pandas import DataFrame, date_range
from lib.concurrent import thread_map
from lib.data_source import DataSource


_api_url_tpl = "https://api-covid19.rnbo.gov.ua/data?to={date}"


def _get_daily_records(date: str):
    records = []
    daily_data = requests.get(_api_url_tpl.format(date=date)).json().get("ukraine", [])
    for record in daily_data:
        records.append(
            {
                "date": date,
                "country_code": "UA",
                "match_string": record.get("label", {}).get("en"),
                "total_confirmed": record.get("confirmed"),
                "total_deceased": record.get("deaths"),
                "total_recovered": record.get("recovered"),
            }
        )
    return records


class UkraineDataSource(DataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        # Data can only be retrieved one day at a time, and it starts on 2020-01-22
        first = "2020-01-22"
        today = datetime.now().date().isoformat()
        map_iter = [str(date)[:10] for date in date_range(first, today)]
        records = sum(thread_map(_get_daily_records, map_iter), [])
        return DataFrame.from_records(records)
