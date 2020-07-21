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
from typing import Dict

from pandas import DataFrame, concat
from lib.data_source import DataSource
from lib.io import read_file
from lib.constants import SRC
from lib.concurrent import thread_map
from lib.time import datetime_isoformat
from lib.utils import table_rename


_column_adapter = {
    "key": "key",
    "date": "date",
    "deces": "total_deceased",
    "testsPositifs": "new_confirmed",
    "testsRealises": "new_tested",
    "gueris": "new_recovered",
    "hospitalises": "current_hospitalized",
    "reanimation": "current_intensive_care",
}

_api_url_tpl = "https://dashboard.covid19.data.gouv.fr/data/code-{}.json"


def _get_department(record: Dict[str, str]):
    subregion1_code = record["subregion1_code"]
    subregion2_code = record["subregion2_code"]
    code = f"DEP-{subregion2_code}"
    data = read_file(_api_url_tpl.format(code))
    data["key"] = f"FR_{subregion1_code}_{subregion2_code}"
    return table_rename(data, _column_adapter, drop=True)


def _get_region(iso_map: Dict[str, str], subregion1_code: str):
    code = iso_map[subregion1_code]
    data = read_file(_api_url_tpl.format(code))
    data["key"] = f"FR_{subregion1_code}"
    return table_rename(data, _column_adapter, drop=True)


class FranceDataSource(DataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        metadata = aux["metadata"]
        metadata = metadata[metadata["country_code"] == "FR"]

        fr_isos = read_file(SRC / "data" / "fr_iso_codes.csv")
        fr_iso_map = {iso: code for iso, code in zip(fr_isos["iso_code"], fr_isos["region_code"])}
        fr_codes = metadata[["subregion1_code", "subregion2_code"]].dropna()
        regions_iter = fr_codes["subregion1_code"].unique()
        deps_iter = (record for _, record in fr_codes.iterrows())

        regions = concat(list(thread_map(partial(_get_region, fr_iso_map), regions_iter)))
        departments = concat(list(thread_map(_get_department, deps_iter, total=len(fr_codes))))

        data = concat([regions, departments])
        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%Y-%m-%d %H:%M:%S"))
        return data
