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
from typing import Dict

from pandas import DataFrame, concat

from lib.cached_data_source import CachedDataSource
from lib.cast import safe_int_cast
from lib.utils import table_rename


class BangladeshDataSource(CachedDataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        dataframes = {}
        date_start = parse_opts.pop("date_start", None)
        date_end = parse_opts.pop("date_end", None)
        for cache_key, cache_urls in sources.items():

            daily_data = []
            for date, url in cache_urls.items():
                if date_start is not None and date < date_start:
                    continue
                if date_end is not None and date > date_end:
                    continue

                with open(url) as fd:
                    data = json.load(fd)
                records = [{"date": date, **row["properties"]} for row in data["features"]]
                daily_data.append(DataFrame.from_records(records))

            dataframes[cache_key] = concat(daily_data)

        return self.parse_dataframes(dataframes, aux, **parse_opts)

    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(
            dataframes["BD_districts_confirmed"],
            {"date": "date", "name": "match_string", "confirmed": "total_confirmed"},
            drop=True,
        )

        data["country_code"] = "BD"
        data["total_confirmed"] = data["total_confirmed"].apply(safe_int_cast)

        return data
