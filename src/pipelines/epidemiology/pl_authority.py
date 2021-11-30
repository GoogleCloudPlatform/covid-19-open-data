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

import zipfile
from io import StringIO
from typing import Dict

from pandas import DataFrame, read_csv, concat

from lib.data_source import DataSource
from lib.utils import table_rename

_column_adapter = {
    # "wojewodztwo": "_voivodeship_name",
    # "powiat_miasto": "_county_name",
    "liczba_przypadkow": "new_confirmed",
    # "liczba_na_10_tys_mieszkancow": "",
    "zgony": "new_deceased",
    # "zgony_w_wyniku_covid_bez_chorob_wspolistniejacych": "",
    # "zgony_w_wyniku_covid_i_chorob_wspolistniejacych": "",
    # "liczba_zlecen_poz": "",
    # "liczba_osob_objetych_kwarantanna": "",
    "liczba_wykonanych_testow": "new_tested",
    # "liczba_testow_z_wynikiem_pozytywnym": "",
    # "liczba_testow_z_wynikiem_negatywnym": "",
    # "liczba_pozostalych_testow": "",
    "teryt": "_code",
    "stan_rekordu_na": "date",
}


def _read_table_from_zipfile(archive: zipfile.ZipFile, fname: str) -> DataFrame:
    table_bytes = archive.read(fname)
    try:
        table_string = table_bytes.decode("Windows-1250")
    except:
        table_string = table_bytes.decode("UTF-8")

    return read_csv(StringIO(table_string), sep=";", encoding="utf8")


class PolandDataSource(DataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        # Data comes as a collection of CSV files inside of a zip archive
        with zipfile.ZipFile(sources[0]) as archive:
            fnames = [info.filename for info in archive.filelist if info.filename.endswith("csv")]
            tables = [_read_table_from_zipfile(archive, fname) for fname in fnames]

        data = table_rename(concat(tables), _column_adapter, drop=True).dropna(subset=["_code"])

        # The data does not start from the beginning so we can't compute cumulative counts
        for col in data.columns:
            total_col = col.replace("new_", "total_")
            if total_col not in data.columns:
                data[total_col] = None

        # Country-level data has a specific label
        country_mask = data["_code"] == "t00"
        country = data.loc[country_mask].copy()
        country["key"] = "PL"

        data = data.loc[~country_mask].copy()

        data["key"] = data["_code"].str.slice(1)
        data["key"] = data["key"].apply(lambda x: x if len(x) == 2 else x[:2] + "_" + x[2:])
        data["key"] = "PL_" + data["key"]

        return concat([country, data])
