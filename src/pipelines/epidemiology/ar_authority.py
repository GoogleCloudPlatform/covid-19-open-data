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
from pandas import DataFrame, concat
from lib.case_line import convert_cases_to_time_series
from lib.cast import numeric_code_as_string
from lib.data_source import DataSource
from lib.utils import table_rename

_ISO_CODE_MAP = {
    "02": "C",  # CABA
    "06": "B",  # Buenos Aires
    "10": "K",  # Catamarca
    "14": "X",  # Córdoba
    "18": "W",  # Corrientes
    "22": "H",  # Chaco
    "26": "U",  # Chubut
    "30": "E",  # Entre Ríos
    "34": "P",  # Formosa
    "38": "Y",  # Jujuy
    "42": "L",  # La Pampa
    "46": "F",  # La Rioja
    "50": "M",  # Mendoza
    "54": "N",  # Misiones
    "58": "Q",  # Neuquén
    "62": "R",  # Río Negro
    "66": "A",  # Salta
    "70": "J",  # San Juan
    "74": "D",  # San Luis
    "78": "Z",  # Santa Cruz
    "82": "S",  # Santa Fe
    "86": "G",  # Santiago del Estero
    "90": "T",  # Tucumán
    "94": "V",  # Tierra del Fuego
}


class ArgentinaDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename appropriate columns
        cases = table_rename(
            dataframes[0],
            {
                "residencia_provincia_id": "subregion1_code",
                "residencia_departamento_id": "subregion2_code",
                "fecha_fallecimiento": "date_new_deceased",
                "fecha_diagnostico": "_date_diagnosed",
                "fecha_internacion": "date_new_hospitalized",
                "fecha_cui_intensivo": "date_new_intensive_care",
                "clasificacion_resumen": "_classification",
                "edad": "age",
                "sexo": "sex",
            },
            drop=True,
        )

        # As long as a case is not labeled as "suspected", assume it has been tested
        cases["date_new_tested"] = None
        suspect_mask = cases["_classification"].str.lower().str.match(".*sospechoso.*")
        cases.loc[~suspect_mask, "date_new_tested"] = cases.loc[suspect_mask, "_date_diagnosed"]

        # Get rid of all the suspected cases, since we have nothing to tally for them
        cases = cases[~suspect_mask]

        # Confirmed cases use the label "confirmado"
        cases["date_new_tested"] = None
        confirmed_mask = cases["_classification"].str.lower().str.match(".*confirmado.*")
        cases.loc[confirmed_mask, "date_new_confirmed"] = cases.loc[
            confirmed_mask, "_date_diagnosed"
        ]

        # Clean up the subregion codes
        cases["subregion1_code"] = cases["subregion1_code"].apply(
            lambda x: None if x == 0 else numeric_code_as_string(x, 2)
        )
        cases["subregion2_code"] = cases["subregion2_code"].apply(
            lambda x: None if x == 0 else numeric_code_as_string(x, 3)
        )

        # Convert subregion1_code to the corresponding ISO code
        cases["subregion1_code"] = cases["subregion1_code"].apply(_ISO_CODE_MAP.get)

        # Remove unnecessary columns before converting to time series
        cases = cases.drop(columns=[col for col in cases.columns if col.startswith("_")])

        # Go from individual case records to key-grouped records in time series format
        data = convert_cases_to_time_series(cases, ["subregion1_code", "subregion2_code"])

        # Parse dates to ISO format.
        data["date"] = data["date"].astype(str)

        # Aggregate by province and report that separately
        provinces = (
            data.drop(columns=["subregion2_code"])
            .groupby(["subregion1_code", "date", "age", "sex"])
            .sum()
            .reset_index()
        )

        # Aggregate to the country level and report that separately
        country = (
            data.drop(columns=["subregion1_code"])
            .groupby(["date", "age", "sex"])
            .sum()
            .reset_index()
        )

        # Compute the key from the subregion codes
        country["key"] = "AR"
        provinces["key"] = "AR_" + provinces["subregion1_code"]
        data["key"] = "AR_" + data["subregion1_code"] + "_" + data["subregion2_code"]

        # Remove bogus values
        for df in (country, provinces, data):
            df.drop(df[df["key"].str.endswith("_")].index, inplace=True)
            for nn_col in ("date", "subregion1_code", "subregion2_code"):
                if nn_col in df.columns:
                    df.drop(df[df[nn_col].isna() | (df[nn_col] == "")].index, inplace=True)

        return concat([data, provinces, country])
