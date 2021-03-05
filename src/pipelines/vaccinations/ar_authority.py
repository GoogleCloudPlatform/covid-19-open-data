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

from functools import partial
from typing import Any, Dict
from bs4 import Tag
from pandas import DataFrame, concat
from lib.cached_data_source import CachedDataSource
from lib.cast import numeric_code_as_string
from lib.data_source import DataSource
from lib.io import read_file
from lib.concurrent import process_map
from lib.utils import table_rename
from pipelines.epidemiology.ar_authority import _ISO_CODE_MAP


_column_adapter = {
    "Date": "date",
    "jurisdiccion_codigo_indec": "subregion1_code",
    "primera_dosis_cantidad": "total_persons_vaccinated",
    "segunda_dosis_cantidad": "total_persons_fully_vaccinated",
}


def _process_cache_file(file_map: Dict[str, str], date: str) -> DataFrame:
    data = table_rename(read_file(file_map[date]), _column_adapter, drop=True)
    data["subregion1_code"] = data["subregion1_code"].apply(
        lambda x: _ISO_CODE_MAP.get(numeric_code_as_string(x, 2) or "00")
    )
    data["date"] = date
    return data


class ArgentinaCachedDataSource(CachedDataSource):
    def parse(
        self, sources: Dict[str, Dict[str, str]], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        file_map = sources["AR_vaccinations"]
        map_func = partial(_process_cache_file, file_map)
        map_opts = dict(desc="Processing Cache Files", total=len(file_map))
        data = concat(process_map(map_func, file_map.keys(), **map_opts))
        assert len(data) > 0, "No records were found"

        # Estimate total doses from first and second doses
        data["total_vaccine_doses_administered"] = (
            data["total_persons_vaccinated"] + data["total_persons_fully_vaccinated"]
        )

        data["key"] = "AR_" + data["subregion1_code"]
        return data
