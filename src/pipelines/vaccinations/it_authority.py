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
from lib.utils import aggregate_admin_level, table_rename
from pipelines.epidemiology.it_authority import _subregion1_code_converter


_column_adapter = {
    "data_somministrazione": "date",
    # "area": "",
    "totale": "new_vaccine_doses_administered",
    # "sesso_maschile": "",
    # "sesso_femminile": "",
    # "categoria_operatori_sanitari_sociosanitari": "",
    # "categoria_personale_non_sanitario": "",
    # "categoria_ospiti_rsa": "",
    # "categoria_over80": "",
    "prima_dose": "new_persons_vaccinated",
    "seconda_dose": "new_persons_fully_vaccinated",
    # "codice_NUTS1": "",
    # "codice_NUTS2": "",
    "codice_regione_ISTAT": "subregion1_code",
    # "nome_area": "subregion1_name",
}


class ItalyDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(dataframes[0], _column_adapter, drop=True).sort_values("date")

        # Convert from the ITSA codes to our region codes
        data["subregion1_code"] = data["subregion1_code"].apply(_subregion1_code_converter)

        # Aggregate here since some of the codes are null (04 indicates either BZ/TN)
        country = aggregate_admin_level(data, ["date"], "country")
        country["key"] = "IT"

        # Match data with IT subregions
        data = data[data['subregion1_code'].notna()]
        data["country_code"] = "IT"
        data["subregion2_code"] = None
        data["locality_code"] = None

        return concat([country, data])
