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

from datetime import datetime
from typing import Any, Dict, List
from pandas import DataFrame, concat, merge
from lib.pipeline import DataSource


_column_map = {
    "data": "date",
    "denominazione_regione": "match_string",
    "ricoverati_con_sintomi": "total_hospitalized_symptomatic",
    "terapia_intensiva": "current_intensive_care",
    "totale_ospedalizzati": "current_hospitalized",
    "isolamento_domiciliare": "total_quarantined",
    "totale_positivi": "current_confirmed",
    "variazione_totale_positivi": "new_current_confirmed",
    "nuovi_positivi": "new_confirmed",
    "dimessi_guariti": "total_recovered",
    "deceduti": "total_deceased",
    "totale_casi": "total_confirmed",
    "tamponi": "total_tested",
    "casi_testati": "cases_tested?",
}


class PcmDpcL1DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = dataframes[0].rename(columns=_column_map)

        # Convert dates to ISO format
        data["date"] = data["date"].apply(lambda x: datetime.fromisoformat(x).date().isoformat())

        # Keep only data we can process
        data = data[[col for col in data.columns if col in _column_map.values()]]

        # We can compute the key directly
        data["key"] = "IT"

        # Output the results
        return data


class PcmDpcL2DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = dataframes[0].rename(columns=_column_map)

        # Convert dates to ISO format
        data["date"] = data["date"].apply(lambda x: datetime.fromisoformat(x).date().isoformat())

        # Keep only data we can process
        data = data[[col for col in data.columns if col in _column_map.values()]]

        # Make sure all records have the country code
        data["country_code"] = "IT"

        # Output the results
        return data
