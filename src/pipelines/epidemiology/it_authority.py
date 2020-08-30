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
from typing import Dict
from pandas import DataFrame
from lib.cast import numeric_code_as_string
from lib.data_source import DataSource
from lib.utils import table_rename


_column_map = {
    "data": "date",
    "codice_regione": "subregion1_code",
    "sigla_provincia": "subregion2_code",
    "denominazione_regione": "subregion1_name",
    "denominazione_provincia": "subregion2_name",
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

# The data source uses some arbitrary codes for regions instead of ISO
_region_code_map = {
    "01": "21",
    "02": "23",
    "03": "25",
    "05": "34",
    "06": "36",
    "07": "42",
    "08": "45",
    "09": "52",
    "10": "55",
    "11": "57",
    "12": "62",
    "13": "65",
    "14": "67",
    "15": "72",
    "16": "75",
    "17": "77",
    "18": "78",
    "19": "82",
    "20": "88",
    "21": "BZ",
    "22": "TN",
}


def _subregion1_code_converter(code: int):
    return _region_code_map.get(numeric_code_as_string(code, 2))


class PcmDpcL1DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        # Rename the appropriate columns
        data = table_rename(dataframes[0], _column_map, drop=True)

        # Convert dates to ISO format
        data["date"] = data["date"].apply(lambda x: datetime.fromisoformat(x).date().isoformat())

        # We can compute the key directly
        data["key"] = "IT"

        # Output the results
        return data


class PcmDpcL2DataSource(PcmDpcL1DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = super().parse_dataframes(dataframes, aux, **parse_opts)

        # Make sure the region codes are strings
        data["subregion1_code"] = data["subregion1_code"].apply(_subregion1_code_converter)

        # Build the keys from the codes in the records
        data["key"] = "IT_" + data["subregion1_code"]

        # Output the results
        return data


class PcmDpcL3DataSource(PcmDpcL2DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = super().parse_dataframes(dataframes, aux, **parse_opts)

        # Build the keys from the codes in the records
        data["key"] = "IT_" + data["subregion1_code"] + "_" + data["subregion2_code"]

        # Remove bogus records
        data = data[data["subregion1_code"].notna()]
        data = data[data["subregion2_code"].notna()]
        data = data[~data["subregion2_code"].isin(["", "AO", "BZ", "TN"])]

        # Output the results
        return data
