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
from typing import Dict

from pandas import DataFrame, concat

from lib.data_source import DataSource
from lib.utils import table_rename
from lib.time import datetime_isoformat

from pipelines.epidemiology.pl_authority import _read_table_from_zipfile

_column_adapter = {
    # "wojewodztwo": "_voivodeship_name",
    # "powiat_miasto": "_county_name",
    "data": "date",
    "teryt": "_code",
    "stan_rekordu_na": "date",
    "liczba_szczepien_ogolnie": "total_vaccine_doses_administered",
    "liczba_szczepien_dziennie": "new_vaccine_doses_administered",
    "dawka_1_dziennie": "new_persons_vaccinated",
    "dawka_1_ogolem": "total_persons_vaccinated",
    "dawka_2_dziennie": "new_persons_fully_vaccinated",
    "dawka_2_ogolem": "total_persons_fully_vaccinated",
    # "dawka_3_ogolem": "total_persons_booster_vaccinated",
    # "dawka_3_dziennie": "new_persons_booster_vaccinated",
    # "dawka_przypominajaca_dziennie": "new_vaccine_booster_administered",
    # "dawka_przypominajaca_ogolem": "total_vaccine_booster_administered",
}


class PolandDataSource(DataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        # Data comes as a collection of CSV files inside of a zip archive
        tables = []
        with zipfile.ZipFile(sources[0]) as archive:
            fnames = [info.filename for info in archive.filelist if info.filename.endswith("csv")]
            for fname in fnames:
                if "woj" in fname or "pow" in fname:
                    table = _read_table_from_zipfile(archive, fname)
                    table["date"] = datetime_isoformat(fname[:8], "%Y%m%d")
                    tables.append(table)

        data = table_rename(concat(tables), _column_adapter, drop=True).dropna(subset=["_code"])

        # Backfill persons vaccinated before the variable was provided
        for prefix in ("new", "total"):
            doses_col = f"{prefix}_vaccine_doses_administered"
            fully_vax_col = f"{prefix}_persons_fully_vaccinated"
            partial_vax_col = f"{prefix}_persons_vaccinated"
            estimate_persons_vax = data[doses_col] - data[fully_vax_col]
            data[partial_vax_col] = data[partial_vax_col].fillna(estimate_persons_vax)

        # Country-level data has a specific label
        country_mask = data["_code"].isin(["t0", "t00", "t0000"])
        country = data.loc[country_mask].copy()
        country["key"] = "PL"

        data = data.loc[~country_mask].copy()

        data["key"] = data["_code"].str.slice(1)
        data["key"] = data["key"].apply(lambda x: x if len(x) == 2 else x[:2] + "_" + x[2:])
        data["key"] = "PL_" + data["key"]

        return concat([country, data])


class PolandCountryDataSource(DataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        # Data comes as a collection of CSV files inside of a zip archive
        suffix = "rap_rcb_daily_szczepienia.csv"
        with zipfile.ZipFile(sources[0]) as archive:
            fnames = [info.filename for info in archive.filelist if info.filename.endswith(suffix)]
            data = _read_table_from_zipfile(archive, fnames[-1])

        data = table_rename(data, _column_adapter, drop=True).dropna(subset=["date"])
        data["key"] = "PL"

        return data
