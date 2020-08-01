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
from lib.cast import numeric_code_as_string
from lib.case_line import convert_cases_to_time_series
from lib.pipeline import DataSource
from lib.utils import table_rename

_SUBREGION1_CODE_MAP = {
    "01": "AGU",
    "02": "BCN",
    "03": "BCS",
    "04": "CAM",
    "05": "COL",
    "06": "COL",
    "07": "CHP",
    "08": "CHH",
    "09": "CMX",
    "10": "DUR",
    "11": "GUA",
    "12": "GRO",
    "13": "HID",
    "14": "JAL",
    "15": "MEX",
    "16": "MIC",
    "17": "MOR",
    "18": "NAY",
    "19": "NLE",
    "20": "OAX",
    "21": "PUE",
    "22": "QUE",
    "23": "ROO",
    "24": "SLP",
    "25": "SIN",
    "26": "SON",
    "27": "TAB",
    "28": "TAM",
    "29": "TLA",
    "30": "VER",
    "31": "YUC",
    "32": "ZAC",
    "36": "TOTAL",
}


def _extract_cities(data: DataFrame) -> DataFrame:
    # TODO: extract cities
    return DataFrame(columns=data.columns)


class MexicoDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        cases = table_rename(
            dataframes[0],
            {
                # "FECHA_ACTUALIZACION": "",
                # "ID_REGISTRO": "",
                # "ORIGEN": "",
                # "SECTOR": "",
                # "ENTIDAD_UM": "",
                "SEXO": "sex",
                # "ENTIDAD_NAC": "",
                "ENTIDAD_RES": "subregion1_code",
                "MUNICIPIO_RES": "subregion2_code",
                "TIPO_PACIENTE": "_type",
                "FECHA_INGRESO": "date_new_confirmed",
                # "FECHA_SINTOMAS": "",
                "FECHA_DEF": "date_new_deceased",
                # "INTUBADO": "",
                # "NEUMONIA": "",
                "EDAD": "age",
                # "NACIONALIDAD": "",
                # "EMBARAZO": "",
                # "HABLA_LENGUA_INDIG": "",
                # "DIABETES": "",
                # "EPOC": "",
                # "ASMA": "",
                # "INMUSUPR": "",
                # "HIPERTENSION": "",
                # "OTRA_COM": "",
                # "CARDIOVASCULAR": "",
                # "OBESIDAD": "",
                # "RENAL_CRONICA": "",
                # "TABAQUISMO": "",
                # "OTRO_CASO": "",
                "RESULTADO": "_diagnosis",
                # "MIGRANTE": "",
                # "PAIS_NACIONALIDAD": "",
                # "PAIS_ORIGEN": "",
                "UCI": "_intensive_care",
            },
            drop=True,
        )

        # Null dates are coded as 9999-99-99
        for col in cases.columns:
            if col.startswith("date_"):
                cases.loc[cases[col] == "9999-99-99", col] = None

        # Discard all cases with negative test result
        cases = cases[cases["_diagnosis"] == 1]

        # Type 1 is normal, type 2 is hospitalized
        cases["date_new_hospitalized"] = None
        hospitalized_mask = cases["_type"] == 2
        cases.loc[hospitalized_mask, "date_new_hospitalized"] = cases.loc[
            hospitalized_mask, "date_new_confirmed"
        ]

        # Parse region codes as strings
        cases["subregion1_code"] = cases["subregion1_code"].apply(
            lambda x: numeric_code_as_string(x, 2)
        )
        cases["subregion2_code"] = cases["subregion2_code"].apply(
            lambda x: numeric_code_as_string(x, 3)
        )

        # Convert case line data to our time series format
        data = convert_cases_to_time_series(cases, ["subregion1_code", "subregion2_code"])

        # Convert date to ISO format
        data["date"] = data["date"].astype(str)

        # Unknown region codes are defined as "99+" instead of null
        data.loc[data["subregion1_code"] == "99", "subregion1_code"] = None
        data.loc[data["subregion2_code"] == "999", "subregion2_code"] = None

        # The subregion2 codes need to be composed
        invalid_region_mask = data["subregion2_code"].isna() | data["subregion2_code"].isna()
        data.loc[~invalid_region_mask, "subregion2_code"] = (
            data.loc[~invalid_region_mask, "subregion1_code"]
            + data.loc[~invalid_region_mask, "subregion2_code"]
        )

        # Use proper ISO codes for the subregion1 level
        data["subregion1_code"] = data["subregion1_code"].apply(_SUBREGION1_CODE_MAP.get)

        # Translate sex labels; only male, female and unknown are given
        data["sex"] = data["sex"].apply(
            lambda x: {"hombre": "male", "mujer": "female"}.get(x.lower())
        )

        # Aggregate state-level data by adding all municipalities
        state = data.drop(columns=["subregion2_code"]).groupby(["date", "subregion1_code"]).sum()
        state.reset_index(inplace=True)
        state["key"] = "MX_" + state["subregion1_code"]

        # Extract cities from the municipalities
        city = _extract_cities(data)

        # Country level is called "TOTAL" as a subregion1_code
        country_mask = data["subregion1_code"] == "TOTAL"
        country = data[country_mask]
        country["key"] = "MX"

        # We can build the key for the data directly from the subregion codes
        data["key"] = "MX_" + data["subregion1_code"] + "_" + data["subregion2_code"]

        # Drop bogus records from the data
        data = data[~country_mask]
        state.dropna(subset=["subregion1_code"], inplace=True)
        data.dropna(subset=["subregion1_code", "subregion2_code"], inplace=True)

        return concat([country, state, data, city])
