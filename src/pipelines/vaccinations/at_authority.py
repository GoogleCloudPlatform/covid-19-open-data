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

from typing import Any, Dict
from pandas import DataFrame, concat
from lib.data_source import DataSource
from lib.time import datetime_isoformat
from lib.utils import aggregate_admin_level, table_rename
from pipelines.epidemiology.it_authority import _subregion1_code_converter


_column_adapter = {
    "Datum": "date",
    "BundeslandID": "_region_code",
    # "Name": "",
    # "Bevölkerung": "",
    # "Auslieferungen": "",
    # "AuslieferungenPro100": "",
    # "Bestellungen": "",
    # "BestellungenPro100": "",
    # "EingetrageneImpfungen": "",
    # "EingetrageneImpfungenPro100": "",
    "Teilgeimpfte": "total_persons_vaccinated",
    # "TeilgeimpftePro100": "",
    "Vollimmunisierte": "total_persons_fully_vaccinated",
    # "VollimmunisiertePro100": "",
}


class AustriaDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(dataframes[0], _column_adapter, drop=True)

        # Convert date to ISO format
        data["date"] = data["date"].str.slice(0, 10)

        # Create the key from the state ID
        data["key"] = data["_region_code"].apply(lambda x: f"AT_{x}")
        data.loc[data["key"] == "AT_10", "key"] = "AT"

        # Estimate the total doses from person counts
        data["total_vaccine_doses_administered"] = (
            data["total_persons_vaccinated"] + data["total_persons_fully_vaccinated"]
        )

        return data
