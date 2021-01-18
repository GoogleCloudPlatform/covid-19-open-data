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
from lib.data_source import DataSource
from lib.utils import table_rename


_province_map = {
    "A": "VC",
    "AB": "CM",
    "AL": "AN",
    "AV": "CL",
    "B": "CT",
    "BA": "EX",
    "BI": "PV",
    "BU": "CL",
    "C": "GA",
    "CA": "AN",
    "CC": "EX",
    "CO": "AN",
    "CR": "CM",
    "CS": "VC",
    "CU": "CM",
    "GC": "CN",
    "GI": "CT",
    "GR": "AN",
    "GU": "CM",
    "H": "AN",
    "HU": "AR",
    "J": "AN",
    "L": "CT",
    "LE": "CL",
    "LO": "RI",
    "LU": "GA",
    "M": "MD",
    "MA": "AN",
    "MU": "MC",
    "NA": "NC",
    "O": "AS",
    "OR": "GA",
    "P": "CL",
    "PM": "IB",
    "PO": "GA",
    "S": "CB",
    "SA": "CL",
    "SE": "AN",
    "SG": "CL",
    "SO": "CL",
    "SS": "PV",
    "T": "CT",
    "TE": "AR",
    "TF": "CN",
    "TO": "CM",
    "V": "VC",
    "VA": "CL",
    "VI": "PV",
    "Z": "AR",
    "ZA": "CL",
}


class ISCIIIConfirmedDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        confirmed = table_rename(
            dataframes[0],
            {"ccaa_iso": "subregion1_code", "fecha": "date", "num_casos": "new_confirmed"},
            drop=True,
        )

        # Convert dates to ISO format
        confirmed["date"] = confirmed["date"].astype(str)

        # Add the country code to all records and declare matching as subregion1
        confirmed["country_code"] = "ES"
        confirmed["subregion2_code"] = None
        confirmed["locality_code"] = None

        # Output the results
        return confirmed


class ISCIIIStratifiedDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = table_rename(
            dataframes[0],
            {
                "provincia_iso": "province",
                "fecha": "date",
                "sexo": "sex",
                "grupo_edad": "age",
                "num_casos": "new_confirmed",
                "num_hosp": "new_hospitalized",
                "num_uci": "new_intensive_care",
                "num_def": "new_deceased",
            },
            drop=True,
        )

        # Convert dates to ISO format
        data["date"] = data["date"].str.slice(0, 10)

        data.loc[data["age"] == "80+", "age"] = "80-"
        data.loc[data["age"] == "NC", "age"] = "age_unknown"
        data["sex"] = data["sex"].apply({"H": "male", "M": "female"}.get).fillna("sex_unknown")

        # Group by country since some regions are N/A
        country = data.groupby(["date", "age", "sex"]).sum().reset_index()
        country["key"] = "ES"

        # Group provinces into autonomous communities (subregion1 level)
        data["subregion1_code"] = data["province"].apply(_province_map.get)
        data = (
            data.drop(columns=["province"])
            .groupby(["date", "subregion1_code", "age", "sex"])
            .sum()
            .reset_index()
        )

        data = data.dropna(subset=["subregion1_code"])
        data["key"] = "ES_" + data["subregion1_code"]

        # Output the results
        return concat([country, data])
