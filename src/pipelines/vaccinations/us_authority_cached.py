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

from functools import partial
from lib.concurrent import process_map
from typing import Dict
from pandas import DataFrame, concat
from lib.cached_data_source import CachedDataSource
from lib.utils import table_rename
from lib.io import read_file


_column_adapter = {"Series_Complete_Yes": "total_persons_fully_vaccinated", "key": "key"}


def _process_cache_file(file_map: Dict[str, str], date: str) -> DataFrame:
    data = read_file(file_map[date])["vaccination_county_condensed_data"].values.tolist()
    data = DataFrame.from_records(data)

    data = data[data["FIPS"] != "UNK"]
    data = data.assign(
        key="US_" + data["StateAbbr"].str[:2] + "_" + data["FIPS"],
        Series_Complete_Yes=data["Series_Complete_Yes"].fillna(0).astype(int),
    )
    data = table_rename(data, _column_adapter, drop=True)

    data["date"] = date
    return data


class USCountyCachedDataSource(CachedDataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        trends_field_name = "US_CDC_counties_vaccinations"
        file_map = sources[trends_field_name]

        map_func = partial(_process_cache_file, file_map)
        map_opts = dict(desc="Processing Cache Files", total=len(file_map))
        data = concat(process_map(map_func, file_map.keys(), **map_opts))
        assert len(data) > 0, "No records were found"

        return data
