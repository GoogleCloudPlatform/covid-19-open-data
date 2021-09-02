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
from pandas import DataFrame
from lib.cast import safe_int_cast
from lib.data_source import DataSource
from lib.utils import table_rename
from pipelines.epidemiology.de_authority import _SUBREGION1_CODE_MAP


_column_adapter = {
    "Date": "date",
    "Department": "match_string_1",
    "City": "match_string_2",
    "Total": "total_vaccine_doses_administered",
}


class FinMangoColombiaDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(dataframes[0], _column_adapter, drop=True)
        int_cols = ["total_vaccine_doses_administered"]
        data = data.dropna(subset=int_cols)
        for col in int_cols:
            data[col] = data[col].apply(safe_int_cast)

        # Fix typos and merge subregions manually
        data["match_string"] = data["match_string_2"].fillna(data["match_string_1"])
        data["match_string"] = data["match_string"].str.replace("Amazionas", "Amazonas")
        data["match_string"] = data["match_string"].str.replace("Baranquilla", "Atlántico")
        data["match_string"] = data["match_string"].str.replace("Benaventura", "Valle del Cauca")
        data["match_string"] = data["match_string"].str.replace("Cartagena", "Bolivar")
        data["match_string"] = data["match_string"].str.replace("Santa Marta", "Magdalena")

        # Match string does not follow strict hierarchy
        data = data.groupby(["date", "match_string"]).sum().reset_index()

        # Make sure only subregion1 level is matched
        data["country_code"] = "CO"
        data["subregion2_code"] = None
        data["locality_code"] = None

        return data
