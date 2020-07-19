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
from pandas import DataFrame, concat, isna
from lib.case_line import convert_cases_to_time_series
from lib.cast import safe_int_cast
from lib.pipeline import DataSource
from lib.time import datetime_isoformat
from lib.utils import table_rename


class CearaDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        cases = table_rename(
            dataframes[0],
            {
                "sexoPaciente": "sex",
                "idadePaciente": "age",
                "codigoMunicipioPaciente": "subregion2_code",
                "dataResultadoExame": "date_new_tested",
                "dataObito": "date_new_deceased",
                "dataEntradaUtisSvep": "date_new_intensive_care",
                "evolucaoCasoSivep": "_prognosis",
                "dataInicioSintomas": "_date_onset",
                "dataEvolucaoCasoSivep": "_date_update",
                "resultadoFinalExame": "_test_result",
            },
            drop=True,
        )

        # Follow the procedure described in the data documentation to compute the confirmed cases:
        # https://drive.google.com/file/d/1DUwST2zcXUnCJmJauiM5zmpSVWqLiAYI/view
        cases["date_new_confirmed"] = None
        confirmed_mask = cases["_test_result"] == "Positivo"
        cases.loc[confirmed_mask, "date_new_confirmed"] = cases.loc[
            confirmed_mask, "date_new_tested"
        ]

        # Only count intensive care patients if they had a positive test result
        cases.loc[~confirmed_mask, "date_new_intensive_care"] = None

        # Drop columns which we have no use for
        cases = cases[[col for col in cases.columns if not col.startswith("_")]]

        # Make sure our region code is of type str
        cases["subregion2_code"] = cases["subregion2_code"].apply(
            lambda x: None if isna(x) else str(safe_int_cast(x))
        )

        # Convert ages to int, and translate sex (no "other" sex/gender reported)
        cases["age"] = cases["age"].apply(safe_int_cast)
        cases["sex"] = cases["sex"].apply({"MASCULINO": "male", "FEMENINO": "female"}.get)

        # Convert to time series format
        data = convert_cases_to_time_series(cases, index_columns=["subregion2_code"])

        # Convert date to ISO format
        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%Y-%m-%d %H:%M:%S"))

        # Aggregate state-level data by adding all municipalities
        state = data.drop(columns=["subregion2_code"]).groupby(["date", "age", "sex"]).sum()
        state.reset_index(inplace=True)
        state["key"] = "BR_CE"

        # Fortaleza is both a subregion of the state and a "locality"
        city = data.loc[data["subregion2_code"] == "230440"].copy()
        city["key"] = "BR_CE_FOR"

        # Drop bogus records from the data
        data = data[~data["subregion2_code"].isna() & (data["subregion2_code"] != "")]

        # We can build the key for the data directly from the subregion code
        data["key"] = "BR_CE_" + data["subregion2_code"]

        return concat([state, data, city])
