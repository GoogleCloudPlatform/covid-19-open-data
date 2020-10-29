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
from pathlib import Path
from typing import Any, Dict, List

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
    "casConfirmes": "total_confirmed",
    "deces": "total_deceased",
    "testsPositifs": "new_confirmed",
    "testsRealises": "new_tested",
    "gueris": "new_recovered",
    "hospitalises": "current_hospitalized",
    "reanimation": "current_intensive_care",
}


def _get_region(url_tpl: str, iso_map: Dict[str, str], subregion1_code: str):
    code = iso_map[subregion1_code]
    data = read_file(url_tpl.format(code))
    data["key"] = f"FR_{subregion1_code}"
    return table_rename(data, _column_adapter, drop=True)


def _get_department(url_tpl: str, record: Dict[str, str]):
    subregion1_code = record["subregion1_code"]
    subregion2_code = record["subregion2_code"]
    code = f"DEP-{subregion2_code}"
    data = read_file(url_tpl.format(code))
    data["key"] = f"FR_{subregion1_code}_{subregion2_code}"
    return table_rename(data, _column_adapter, drop=True)


def _get_country(url_tpl: str):
    data = read_file(url_tpl.format("FRA"))
    data["key"] = "FR"
    # For country level, there is no need to estimate confirmed from tests
    _column_adapter_2 = dict(_column_adapter)
    _column_adapter_2.pop("testsPositifs")
    return table_rename(data, _column_adapter_2, drop=True)


class FranceDataSource(DataSource):
    def fetch(
        self, output_folder: Path, cache: Dict[str, str], fetch_opts: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        # URL is just a template, so pass-through the URL to parse manually
        return {idx: source["url"] for idx, source in enumerate(fetch_opts)}

    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        url_tpl = sources[0]
        metadata = aux["metadata"]
        metadata = metadata[metadata["country_code"] == "FR"]

        fr_isos = read_file(SRC / "data" / "fr_iso_codes.csv")
        fr_iso_map = {iso: code for iso, code in zip(fr_isos["iso_code"], fr_isos["region_code"])}
        fr_codes = metadata[["subregion1_code", "subregion2_code"]].dropna()
        regions_iter = fr_codes["subregion1_code"].unique()
        deps_iter = (record for _, record in fr_codes.iterrows())

        if parse_opts.get("country"):
            data = _get_country(url_tpl)

        else:
            get_region_func = partial(_get_region, url_tpl, fr_iso_map)
            regions = concat(list(thread_map(get_region_func, regions_iter)))

            get_department_func = partial(_get_department, url_tpl)
            departments = concat(
                list(thread_map(get_department_func, deps_iter, total=len(fr_codes)))
            )

            data = concat([regions, departments])

        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%Y-%m-%d %H:%M:%S"))
        return data
