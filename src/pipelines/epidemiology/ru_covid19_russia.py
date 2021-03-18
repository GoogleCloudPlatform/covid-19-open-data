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
from typing import Any, Dict
from pandas import DataFrame
from lib.io import read_file
from lib.data_source import DataSource
from lib.utils import pivot_table


_column_adapter = {
    "confirmed": "total_confirmed",
    "deaths": "total_deceased",
    "recovered": "total_recovered",
}


class RussiaCovid19DataSource(DataSource):
    def parse(self, sources: Dict[Any, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        with open(sources[0], "r") as fh:
            data = json.load(fh)

        records = []
        for subregion1_name, timeseries in data.items():
            record = {
                "country_code": "RU",
                "subregion2_code": None,
                "locality_code": None,
                "match_string": subregion1_name.lower(),
            }
            for row in timeseries:
                records.append(dict(record, **row))

        cast_cols = list(_column_adapter.values())
        data = DataFrame.from_records(records).rename(columns=_column_adapter)
        data[cast_cols] = data[cast_cols].astype(int)
        return data
