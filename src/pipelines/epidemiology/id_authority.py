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

import datetime
import json
import requests
from functools import partial
from pathlib import Path
from typing import Dict, List, Any
from pandas import DataFrame
from lib.cast import safe_int_cast
from lib.concurrent import thread_map
from lib.data_source import DataSource

_key_name_map = {
    "ID_JK": "DKI JAKARTA",
    "ID_JB": "JAWA BARAT",
    "ID_JI": "JAWA TIMUR",
    "ID_JT": "JAWA TENGAH",
    "ID_SN": "SULAWESI SELATAN",
    "ID_BT": "BANTEN",
    "ID_NB": "NUSA TENGGARA BARAT",
    "ID_BA": "BALI",
    "ID_PA": "PAPUA",
    "ID_KS": "KALIMANTAN SELATAN",
    "ID_SB": "SUMATERA BARAT",
    "ID_SS": "SUMATERA SELATAN",
    "ID_KT": "KALIMANTAN TENGAH",
    "ID_KI": "KALIMANTAN TIMUR",
    "ID_SU": "SUMATERA UTARA",
    "ID_YO": "DAERAH ISTIMEWA YOGYAKARTA",
    "ID_KU": "KALIMANTAN UTARA",
    "ID_KR": "KEPULAUAN RIAU",
    "ID_KB": "KALIMANTAN BARAT",
    "ID_SG": "SULAWESI TENGGARA",
    "ID_LA": "LAMPUNG",
    "ID_SA": "SULAWESI UTARA",
    "ID_ST": "SULAWESI TENGAH",
    "ID_RI": "RIAU",
    "ID_PB": "PAPUA BARAT",
    "ID_SR": "SULAWESI BARAT",
    "ID_JA": "JAMBI",
    "ID_MU": "MALUKU UTARA",
    "ID_MA": "MALUKU",
    "ID_GO": "GORONTALO",
    "ID_BB": "KEPULAUAN BANGKA BELITUNG",
    "ID_AC": "ACEH",
    "ID_BE": "BENGKULU",
    "ID_NT": "NUSA TENGGARA TIMUR",
}

_col_name_map = {
    "key": "date",
    "tanggal": "date",
    "KASUS": "new_confirmed",
    "jumlah_positif": "new_confirmed",
    "MENINGGAL": "new_deceased",
    "jumlah_meninggal": "new_deceased",
    "SEMBUH": "new_recovered",
    "jumlah_sembuh": "new_recovered",
    "AKUMULASI_KASUS": "total_confirmed",
    "jumlah_positif_kum": "total_confirmed",
    "AKUMULASI_MENINGGAL": "total_deceased",
    "jumlah_meninggal_kum": "total_deceased",
    "AKUMULASI_SEMBUH": "total_recovered",
    "jumlah_sembuh_kum": "total_recovered",
}


def _parse_value(val: Any) -> int:
    if isinstance(val, dict):
        return _parse_value(val["value"])
    else:
        return safe_int_cast(val)


def _parse_records(key: str, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    parsed_records = []
    for record in records:
        _record = {col: key for col, key in record.items() if col in _col_name_map}
        _record = {_col_name_map[col]: _parse_value(val) for col, val in _record.items()}
        date = datetime.datetime.fromtimestamp(_record["date"] // 1000)
        _record.update({"key": key, "date": date.date().isoformat()})
        parsed_records.append(_record)
    return parsed_records


def _get_province_records(url_tpl: str, key: str) -> List[Dict[str, Any]]:
    url = url_tpl.format(_key_name_map[key].replace(" ", "_"))
    return _parse_records(key, requests.get(url, timeout=60).json()["list_perkembangan"])


# pylint: disable=missing-class-docstring,abstract-method
class IndonesiaProvinceDataSource(DataSource):
    def fetch(
        self,
        output_folder: Path,
        cache: Dict[str, str],
        fetch_opts: List[Dict[str, Any]],
        skip_existing: bool = False,
    ) -> Dict[str, str]:
        # URL is just a template, so pass-through the URL to parse manually
        return {idx: source["url"] for idx, source in enumerate(fetch_opts)}

    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        # Ignore sources, we use an API for this data source
        url_tpl = sources[0]
        keys = aux["metadata"].query('(country_code == "ID") & subregion1_code.notna()')["key"]
        keys = [key for key in keys.values if len(key.split("_")) == 2 and len(key) == 5]

        map_func = partial(_get_province_records, url_tpl)
        data = DataFrame.from_records(sum(thread_map(map_func, keys), []))
        return data


class IndonesiaCountryDataSource(DataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        with open(sources[0]) as fd:
            records = json.load(fd)["update"]["harian"]
        return DataFrame.from_records(_parse_records("ID", records))
