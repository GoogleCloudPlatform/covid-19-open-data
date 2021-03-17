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

import datetime
import requests
from functools import partial
from pathlib import Path
from typing import Dict, List, Any
from pandas import DataFrame
from lib.concurrent import thread_map
from lib.data_source import DataSource
from lib.utils import table_merge
from lib.utils import table_rename

_subregioncode_to_api_id_map = {
    "AC": 1,
    "BA": 2,
    "BT": 3,
    "BE": 4,
    "YO": 5,
    "JK": 6,
    "GO": 7,
    "JA": 8,
    "JB": 9,
    "JT": 10,
    "JI": 11,
    "KB": 12,
    "KS": 13,
    "KT": 14,
    "KI": 15,
    "KU": 16,
    "BB": 17,
    "KR": 18,
    "LA": 19,
    "MA": 20,
    "MU": 21,
    "NB": 22,
    "NT": 23,
    "PA": 24,
    "PB": 25,
    "RI": 26,
    "SR": 27,
    "SN": 28,
    "ST: 29,"
    "SG": 30,
    "SA": 31,
    "SB": 32,
    "SS": 33,
    "SU": 34,
    "3523": 160,
    "3504": 161,
}

_col_name_map = {
    "date": "date",
    "key": "key",
    "kasus": "total_confirmed",
    "kasus_baru": "new_confirmed",
    "kematian": "total_deceased",
    "kematian_baru": "new_deceased",
    "sembuh": "total_recovered",
    "sembuh_perhari": "new_recovered",
}


def _get_records(url_tpl: str, subregion_code: str) -> List[Dict[str, Any]]:
    url = url_tpl.format(_subregioncode_to_api_id_map[subregion_code])
    res = requests.get(url, timeout=60).json()
    records = list(res.values())
    [s.update({"subregion_code": subregion_code}) for s in records]
    return records


def _indonesian_date_to_isoformat(indo_date: str) -> str:
    """ Convert date like '18 Desember 2020' or '31 JulI 2020' to iso format"""
    indonesian_to_english_months = {
        "januari": "Jan",
        "februari": "Feb",
        "maret": "Mar",
        "april": "Apr",
        "mei": "May",
        "juni": "Jun",
        "juli": "Jul",
        "agustus": "Aug",
        "september": "Sep",
        "oktober": "Oct",
        "november": "Nov",
        "desember": "Dec",
    }
    eng_date = indo_date.lower()
    for indo, eng in indonesian_to_english_months.items():
        eng_date = eng_date.replace(indo, eng)
    date = datetime.datetime.strptime(eng_date, "%d %b %Y")
    return date.date().isoformat()


def _get_subregions_data(url_tpl: str, subregion_code_col, subregions: DataFrame) -> DataFrame:
    subregion_codes = subregions[subregion_code_col].values
    map_func = partial(_get_records, url_tpl)
    data = DataFrame.from_records(sum(thread_map(map_func, subregion_codes), []))
    # add location keys
    data['date'] = data.apply(lambda r: _indonesian_date_to_isoformat(r.tgl), axis=1)
    data = table_merge(
        [data, subregions],
        left_on="subregion_code", right_on=subregion_code_col, how="left")
    data = table_rename(data, _col_name_map, drop=True)
    return data


# pylint: disable=missing-class-docstring,abstract-method
class IndonesiaAndrafarmDataSource(DataSource):
    def fetch(
        self,
        output_folder: Path,
        cache: Dict[str, str],
        fetch_opts: List[Dict[str, Any]],
        skip_existing: bool = False,
    ) -> Dict[str, str]:
        # URL is just a template, so pass-through the URL to parse manually
        return {source["name"]: source["url"] for source in fetch_opts}

    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        # subregion1s = aux["metadata"].query('(country_code == "ID") & subregion1_code.notna()')  # type: Dataframe
        subregion2s = aux["metadata"].query('(country_code == "ID") & subregion2_code.notna()')  # type: Dataframe
        data = _get_subregions_data(sources['level2_url'], 'subregion2_code', subregion2s)
        # data = concat([
        #     _get_subregions_data(sources['province_url'], 'subregion1_code', subregion1s),
        #     _get_subregions_data(sources['level2_url'], 'subregion2_code', subregion2s),
        # ])
        return data
