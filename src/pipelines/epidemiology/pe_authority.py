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
from lib.cast import safe_int_cast, safe_str_cast
from lib.data_source import DataSource
from lib.case_line import convert_cases_to_time_series
from lib.time import datetime_isoformat
from lib.utils import table_multimerge, table_rename

_column_adapter = {
    "FECHA_RESULTADO": "date",
    "FECHA_FALLECIMIENTO": "date",
    "DISTRITO": "subregion2_name",
    "DEPARTAMENTO": "subregion1_name",
    "PROVINCIA": "province_name",
    "EDAD": "age",
    "EDAD_DECLARADA": "age",
    "SEXO": "sex",
}

# Used only for departments with mismatching names or special characters
_department_map = {
    "ANCASH": "Áncash",
    "APURIMAC": "Apurímac",
    "HUANUCO": "Huánuco",
    "JUNIN": "Junín",
    "LIMA": "Metropolitan Municipality of Lima",
    "LIMA REGION": "Lima Department",
    "MADRE DE DIOS": "Madre de Dios",
    "SAN MARTIN": "San Martín",
}


class PeruDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        cases_confirmed = table_rename(dataframes["confirmed"], _column_adapter, drop=True).rename(
            columns={"date": "date_new_confirmed"}
        )
        cases_deceased = table_rename(dataframes["deceased"], _column_adapter, drop=True).rename(
            columns={"date": "date_new_deceased"}
        )

        # Translate sex label
        for df in (cases_confirmed, cases_deceased):
            df["sex"] = df["sex"].apply({"MASCULINO": "male", "FEMENINO": "female"}.get)

        # Convert to time series
        index_columns = ["subregion1_name", "province_name", "subregion2_name"]
        data_confirmed = convert_cases_to_time_series(cases_confirmed, index_columns)
        data_deceased = convert_cases_to_time_series(cases_deceased, index_columns)

        # Join into a single dataset
        data = table_multimerge([data_confirmed, data_deceased], how="outer")

        # Remove bogus records
        data.dropna(subset=["date"], inplace=True)

        # Set country code and get date in ISO format
        data["country_code"] = "PE"
        data["date"] = data["date"].apply(safe_int_cast)
        data["date"] = data["date"].apply(safe_str_cast)
        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%Y%m%d"))

        # Properly capitalize department to allow for exact matching
        data["subregion1_name"] = data["subregion1_name"].apply(
            lambda x: _department_map.get(x, x.title())
        )

        # Aggregate by admin level 1
        subregion1 = (
            data.drop(columns=["subregion2_name", "province_name"])
            .groupby(["date", "country_code", "subregion1_name", "age", "sex"])
            .sum()
            .reset_index()
        )
        subregion1["subregion2_name"] = None

        # Try to match based on subregion2_name using fuzzy matching, and set subregion2_name to
        # an empty string to turn off exact matching
        data = data.rename(columns={"subregion2_name": "match_string"})
        data["subregion2_name"] = ""

        # Drop bogus records
        data = data[~data["match_string"].isna()]
        data = data[~data["match_string"].isin(["", "EN INVESTIGACIÓN", "EXTRANJERO"])]

        # Because we are skipping provinces and going directly from region to district, there are
        # some name collisions which we have to disambiguate manually
        for province1, province2, district in [
            ("lima", "canete", "sanluis"),
            ("lima", "yauyos", "miraflores"),
            ("ica", "chincha", "pueblonuevo"),
            ("canete", "huarochiri", "sanantonio"),
            ("bolognesi", "huaylas", "huallanca"),
        ]:
            for province in (province1, province2):
                mask = (data["province_name"] == province) & (data["match_string"] == district)
                data.loc[mask, "match_string"] = f"{district}, {province}"

        # Output the results
        return concat([subregion1, data])
