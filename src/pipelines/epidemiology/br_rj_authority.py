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

from typing import Dict
from pandas import DataFrame
from lib.case_line import convert_cases_to_time_series
from lib.pipeline import DataSource
from lib.time import datetime_isoformat
from lib.utils import table_rename


class RioStratifiedDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        cases = table_rename(
            dataframes[0],
            {
                "classificação_final": "confirmed_label",
                "dt_notific": "date_new_confirmed",
                # "dt_inicio_sintomas": "_date_onset",
                "bairro_resid__estadia": "match_string",
                # "ap_residencia_estadia": "_health_department_code",
                "sexo": "sex",
                "faixa_etária": "age",
                "evolução": "_state_label",
                "dt_óbito": "date_new_deceased",
                "raça/cor": "ethnicity",
                "Data_atualização": "_date_updated",
            },
            drop=True,
        )

        # Currently active cases are those which are labeled as "ativo" with the report's date
        cases["date_current_confirmed"] = None
        report_date = cases["_date_updated"].iloc[0]
        cases.loc[cases["_state_label"] == "ativo", "date_current_confirmed"] = report_date

        # Drop columns which we have no use for
        cases = cases[[col for col in cases.columns if not col.startswith("_")]]

        # Age is already in buckets
        cases["age"] = cases["age"].apply(lambda x: x.replace("De ", "").replace(" a ", "-"))

        # Make all unknown ages null
        cases.loc[cases["age"].str.contains("N/D"), "age"] = None

        # Ethnicity needs translation
        cases["ethnicity"] = cases["ethnicity"].apply(
            lambda x: {"preta": "black", "parda": "mixed", "branca": "white"}.get(
                str(x).lower(), "unknown"
            )
        )

        data = convert_cases_to_time_series(cases, index_columns=["match_string"])
        data["country_code"] = "BR"
        data["subregion1_code"] = "RJ"

        # Convert date to ISO format
        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%d/%m/%y"))

        # The sum of all districts is the metropolitan area of Rio
        metro = data.groupby(["date", "age", "sex", "ethnicity"]).sum().reset_index()
        metro["key"] = "BR_RJ_3304557"

        # Rio is both a subregion of the state and a "locality"
        city = metro.copy()
        city["key"] = "BR_RJ_GIG"

        # Remove bogus data
        data = data[data.match_string != "INDEFINIDO"]
        data = data[data.match_string != "FORA DO MUNICÍPIO"]

        # Return only city-level data for now
        # TODO(owahltinez): add the rest of the data once statewide districts are reported
        # return concat([city, metro, data])
        return city
