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

from pathlib import Path
from typing import Any, Dict, List

from pandas import DataFrame

from lib.data_source import DataSource
from lib.time import date_range, date_today


class _PeruDataSource(DataSource):
    def fetch(
        self,
        output_folder: Path,
        cache: Dict[str, str],
        fetch_opts: List[Dict[str, Any]],
        skip_existing: bool = False,
    ) -> Dict[str, str]:
        # Data is published as daily snapshots, so we guess the URL based on the date
        opts = dict(fetch_opts[0])
        url_tpl = opts.pop("url")

        urls = []
        date_start = "2020-05-06"
        date_end = date_today(offset=1)
        for date in date_range(date_start, date_end):
            datestr = "".join(reversed(date.split("-")))
            urls.append(dict(name=date, url=url_tpl.format(date=datestr), **opts))

        # Pass the actual URLs down to fetch it
        return super().fetch(output_folder, cache, urls, skip_existing=skip_existing)


class PeruHospitalizedDataSource(_PeruDataSource):
    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        records = []
        for date, df in dataframes.items():
            record = {"key": "PE"}
            record["date"] = date
            record["current_hospitalized"] = df.loc[
                df["CATEGORIA"] == "USO DE VENTILACION MECÁNICA", "TOTAL"
            ].sum()
            record["current_ventilator"] = df.loc[
                df["DETALLE"] == "CON VENTILACIÓN MECÁNICA", "TOTAL"
            ].sum()
            records.append(record)

        return DataFrame.from_records(records)


class PeruICUDataSource(_PeruDataSource):
    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        records = []
        for date, df in dataframes.items():
            record = {"key": "PE"}
            record["date"] = date
            record["current_intensive_care"] = df["Disponible"].sum()
            records.append(record)

        return DataFrame.from_records(records)
