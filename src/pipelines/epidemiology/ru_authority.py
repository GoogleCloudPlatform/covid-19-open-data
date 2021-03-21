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

import requests
from functools import partial
from pathlib import Path
from typing import Dict, List, Any
from pandas import DataFrame
from lib.cast import safe_int_cast
from lib.concurrent import thread_map
from lib.data_source import DataSource
from lib.time import datetime_isoformat

_api_url_tpl = "https://xn--80aesfpebagmfblc0a.xn--p1ai/covid_data.json?do=region_stats&code={}"


def _get_province_records(url_tpl: str, key: str) -> List[Dict[str, Any]]:
    records = []
    url = url_tpl.format(key.replace("_", "-"))
    for record in requests.get(url, timeout=60).json():
        records.append(
            {
                "key": key,
                "date": datetime_isoformat(record["date"], "%d.%m.%Y"),
                "total_confirmed": safe_int_cast(record["sick"]),
                "total_deceased": safe_int_cast(record["died"]),
                "total_recovered": safe_int_cast(record["healed"]),
            }
        )
    return records


# pylint: disable=missing-class-docstring,abstract-method
class RussiaDataSource(DataSource):
    def fetch(
        self,
        output_folder: Path,
        cache: Dict[str, str],
        fetch_opts: List[Dict[str, Any]],
        skip_existing: bool = False,
    ) -> Dict[Any, str]:
        # URL is just a template, so pass-through the URL to parse manually
        return {idx: source["url"] for idx, source in enumerate(fetch_opts)}

    def parse(self, sources: Dict[Any, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        # Get a list of all keys to query the API with
        keys = aux["metadata"].query('country_code == "RU"').key
        keys = [key for key in keys.values if len(key.split("_")) == 2]

        map_func = partial(_get_province_records, sources[0])
        data = DataFrame.from_records(sum(thread_map(map_func, keys), []))
        data = data[["key", "date", "total_confirmed", "total_deceased", "total_recovered"]]
        return data
