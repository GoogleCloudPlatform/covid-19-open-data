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

from pathlib import Path
from typing import Any, Dict, List

from pandas import DataFrame, concat

from lib.data_source import DataSource
from lib.time import date_range, date_today, date_offset, datetime_isoformat
from lib.utils import table_rename


_column_adapter = {
    "Unnamed: 0": "match_string",
    "Dosis entregadas (1)": "total_vaccine_doses_deployed",
    "Dosis administradas (2)": "total_vaccine_doses_administered",
    "Nº Personas vacunadas": "total_persons_fully_vaccinated",
    "Nº Personas vacunadas (pauta completada)": "total_persons_fully_vaccinated",
    "Fecha de la última vacuna registrada (2)": "date",
}


class SpainDataSource(DataSource):
    def fetch(
        self,
        output_folder: Path,
        cache: Dict[str, str],
        fetch_opts: List[Dict[str, Any]],
        skip_existing: bool = False,
    ) -> Dict[str, str]:
        # Data is published as GitHub Releases, so we guess the URL based on the date
        opts = dict(fetch_opts[0])
        url_tpl = opts.pop("url")

        urls = []
        date_start = "2021-01-11"
        date_end = date_today(offset=1)
        for date in date_range(date_start, date_end):
            urls.append(dict(name=date, url=url_tpl.format(date=date.replace("-", "")), **opts))

        # Pass the actual URLs down to fetch it
        return super().fetch(output_folder, cache, urls, skip_existing=skip_existing)

    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        tables = []
        for date, df in dataframes.items():
            df = table_rename(df, _column_adapter, drop=True, remove_regex=r"[^a-z]")

            # Fill the date when blank
            df["date"] = df["date"].fillna(df["date"].max())
            df["date"] = df["date"].apply(lambda x: x.date().isoformat())

            # Correct the obvious date typos
            df["date"] = df["date"].apply(lambda x: x.replace("2022", "2021"))

            tables.append(df)

        data = concat(tables)

        # Estimate first doses from total doses and second doses
        data["total_persons_vaccinated"] = (
            data["total_vaccine_doses_administered"] - data["total_persons_fully_vaccinated"]
        )

        data["key"] = None
        data["country_code"] = "ES"
        data["subregion2_code"] = None
        data["locality_code"] = None

        data.loc[data["match_string"] == "Totales", "key"] = "ES"

        return data
