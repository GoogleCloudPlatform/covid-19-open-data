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

from typing import Any, Dict, List
from pathlib import Path

import requests
from pandas import DataFrame

from lib.cast import safe_int_cast
from lib.data_source import DataSource
from lib.utils import table_merge, table_rename


_column_adapter = {"datum": "date", "geoRegion": "subregion1_code"}


class SwitzerlandCantonsDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[Any, DataFrame], **parse_opts
    ) -> DataFrame:
        data = (
            dataframes[0]
            .rename(
                columns={
                    "ncumul_tested": "total_tested",
                    "ncumul_conf": "total_confirmed",
                    "ncumul_deceased": "total_deceased",
                    "ncumul_hosp": "total_hospitalized",
                    "ncumul_ICU": "total_intensive_care",
                    "ncumul_vent": "total_ventilator",
                    "ncumul_released": "total_recovered",
                    "abbreviation_canton_and_fl": "subregion1_code",
                }
            )
            .drop(columns=["time", "source"])
        )

        # We can derive the key from country code + subregion1 code
        data["key"] = "CH_" + data["subregion1_code"]

        # Principality of Liechtenstein is not in CH
        data.loc[data["subregion1_code"] == "FL", "key"] = "LI"

        return data


class SwitzerlandAdminDataSource(DataSource):
    def fetch(
        self,
        output_folder: Path,
        cache: Dict[str, str],
        fetch_opts: List[Dict[str, Any]],
        skip_existing: bool = False,
    ) -> Dict[str, str]:
        # the url in the config is a json file which contains the actual dated urls for the data
        src_url = fetch_opts[0]["url"]
        data = requests.get(src_url).json()
        csv_data = data["sources"]["individual"]["csv"]

        fetch_opts = []
        col_name_map = {
            "cases": "new_confirmed",
            "death": "new_deceased",
            "test": "new_tested",
            "hosp": "new_hospitalized",
        }
        for api_name, col_name in col_name_map.items():
            fetch_opts.append({"name": col_name, "url": csv_data["daily"][api_name]})

        return super().fetch(output_folder, cache, fetch_opts, skip_existing)

    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        # Convert the raw data into numeric values
        for df in dataframes.values():
            df["entries"] = df["entries"].apply(safe_int_cast)

        data = table_merge(
            [
                table_rename(df, dict(_column_adapter, entries=name), drop=True)
                for name, df in dataframes.items()
            ],
            on=["date", "subregion1_code"],
            how="outer",
        )

        # Make sure all records have the country code and match subregion1 only
        data["key"] = None
        data["country_code"] = "CH"
        data["subregion2_code"] = None
        data["locality_code"] = None

        # Country-level records have a known key
        country_mask = data["subregion1_code"] == "CH"
        data.loc[country_mask, "key"] = "CH"

        # Principality of Liechtenstein is not in CH but is in the data as FL
        country_mask = data["subregion1_code"] == "FL"
        data.loc[country_mask, "key"] = "LI"

        # Output the results
        return data
