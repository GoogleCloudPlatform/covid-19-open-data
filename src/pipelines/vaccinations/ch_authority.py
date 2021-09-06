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
import requests
from pandas import DataFrame
from lib.data_source import DataSource
from lib.utils import table_merge
from lib.utils import table_rename


def _output_ch_data(data: DataFrame) -> DataFrame:
    # Make sure all records have the country code and match subregion1 only
    data["key"] = None
    data["country_code"] = "CH"
    data["subregion2_code"] = None
    data["locality_code"] = None

    # Country-level records have a known key
    country_mask = data["subregion1_code"] == "CH"
    data.loc[country_mask, "key"] = "CH"

    # Principality of Liechtenstein is not in CH but is in the data as FL
    data.loc[data["subregion1_code"] == "FL", "key"] = "LI"

    # Output the results
    return data


class SwitzerlandPersonsDataSource(DataSource):
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

        fetch_opts = [
            {
                "name": "fullyVaccPersons",
                "url": data["sources"]["individual"]["csv"]["vaccPersonsV2"],
            }
        ]

        return super().fetch(output_folder, cache, fetch_opts, skip_existing)

    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(
            dataframes["fullyVaccPersons"],
            {
                "date": "date",
                "geoRegion": "subregion1_code",
                "type": "_statistic",
                "entries": "_new_count",
                "sumTotal": "_total_count",
            },
            drop=True,
        )

        # Combine all the different variable indicators
        tables = []
        for col, var in {
            "COVID19AtLeastOneDosePersons": "persons_vaccinated",
            "COVID19FullyVaccPersons": "persons_fully_vaccinated",
        }.items():
            adapter = {"_new_count": f"new_{var}", "_total_count": f"total_{var}"}
            subset = data.loc[data["_statistic"] == col].drop(columns=["_statistic"])
            tables.append(subset.rename(columns=adapter))
        data = table_merge(tables, on=["date", "subregion1_code"], how="outer")

        # Output the results
        return _output_ch_data(data)


class SwitzerlandDosesDataSource(DataSource):
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

        fetch_opts = [
            {
                "name": "vaccDosesAdministered",
                "url": data["sources"]["individual"]["csv"]["vaccDosesAdministered"],
            }
        ]

        return super().fetch(output_folder, cache, fetch_opts, skip_existing)

    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(
            dataframes["vaccDosesAdministered"],
            {
                "date": "date",
                "geoRegion": "subregion1_code",
                "entries": "new_vaccine_doses_administered",
                "sumTotal": "total_vaccine_doses_administered",
            },
            drop=True,
        )

        # Output the results
        return _output_ch_data(data)
