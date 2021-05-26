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
from lib.data_source import DataSource
from lib.utils import aggregate_admin_level, table_rename
from pipelines.epidemiology.ar_authority import _ISO_CODE_MAP


class ArgentinaDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename appropriate columns
        data = table_rename(
            dataframes[0],
            {
                "sexo": "sex",
                "grupo_etario": "age",
                # "jurisdiccion_residencia": "",
                "jurisdiccion_residencia_id": "subregion1_code",
                # "depto_residencia": "",
                "depto_residencia_id": "subregion2_code",
                # "jurisdiccion_aplicacion": "",
                # "jurisdiccion_aplicacion_id": "",
                # "depto_aplicacion": "",
                # "depto_aplicacion_id": "",
                "fecha_aplicacion": "date",
                "vacuna": "_manufacturer",
                # "condicion_aplicacion": "",
                "orden_dosis": "_dose_number",
                # "lote_vacuna": "",
            },
            drop=True,
        )

        # Parse dates to ISO format.
        data["date"] = data["date"].astype(str)

        # Parse sex label into proper name
        data["sex"] = data["sex"].apply({"M": "male", "F": "female"}.get)

        # Parse the dose number assuming all vaccines have a 2-dose schedule
        data["new_persons_vaccinated"] = data["_dose_number"].apply(lambda x: 1 if x == 1 else 0)
        data["new_persons_fully_vaccinated"] = data["_dose_number"].apply(
            lambda x: 1 if x == 2 else 0
        )
        data["new_vaccine_doses_administered"] = (
            data["new_persons_vaccinated"] + data["new_persons_fully_vaccinated"]
        )

        # Add a column for each vaccine manufacturer
        for manufacturer in data["_manufacturer"].unique():
            mask = data["_manufacturer"] == manufacturer
            brand_name = manufacturer.lower()

            cols = [f"new_persons_{mod}vaccinated" for mod in ["", "fully_"]]
            cols += [f"new_vaccine_doses_administered"]
            for col in cols:
                new_col = f"{col}_{brand_name}"
                data[new_col] = None
                data.loc[mask, new_col] = data.loc[mask, col]

        # Clean up the subregion codes
        data["subregion1_code"] = data["subregion1_code"].apply(
            lambda x: numeric_code_as_string(x, 2) or "00"
        )
        data["subregion2_code"] = data["subregion2_code"].apply(
            lambda x: numeric_code_as_string(x, 3) or "000"
        )

        # Convert subregion1_code to the corresponding ISO code
        data["subregion1_code"] = data["subregion1_code"].apply(_ISO_CODE_MAP.get)

        # Group by indexable columns
        idx_cols = ["date", "subregion1_code", "subregion2_code", "sex", "age"]
        data = data.groupby(idx_cols).sum().reset_index()

        # Aggregate country level and admin level 1
        country = aggregate_admin_level(data, ["date", "age", "sex"], "country")
        subregion1 = aggregate_admin_level(data, ["date", "age", "sex"], "subregion1")
        subregion2 = data.copy()

        # Drop regions without a code
        subregion2 = subregion2[subregion2["subregion2_code"] != "000"]
        subregion2.dropna(subset=["subregion1_code", "subregion2_code"], inplace=True)
        subregion1.dropna(subset=["subregion1_code"], inplace=True)

        # Compute the key from the subregion codes
        country["key"] = "AR"
        subregion1["key"] = "AR_" + subregion1["subregion1_code"]
        subregion2["key"] = "AR_" + subregion2["subregion1_code"] + "_" + data["subregion2_code"]

        return concat([subregion2, subregion1, country])
