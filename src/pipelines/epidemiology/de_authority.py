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
from lib.pipeline import DataSource
from lib.utils import table_rename


_SUBREGION1_CODE_MAP = {
    1: "SH",
    2: "HH",
    3: "NI",
    4: "HB",
    5: "NW",
    6: "HE",
    7: "RP",
    8: "BW",
    9: "BY",
    10: "SL",
    11: "BE",
    12: "BB",
    13: "MV",
    14: "SN",
    15: "ST",
    16: "TH",
}


def _parse_age_bin(age_bin: str) -> str:
    age_bin = age_bin.replace("A", "").replace("+", "-")
    try:
        tokens = age_bin.split("-", 1)
        lo = int(tokens[0])
        hi = int(tokens[1]) if len(tokens) == 2 and len(tokens[1]) > 0 else ""
        return f"{lo}-{hi}"
    except ValueError:
        return "age_unknown"


class GermanyDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = table_rename(
            dataframes[0],
            {
                # "ObjectId": "",
                # "Bundesland": "subregion1_name",
                "IdBundesland": "subregion1_code",
                # "Landkreis": "subregion2_name",
                "IdLandkreis": "subregion2_code",
                "Altersgruppe": "age",
                "Geschlecht": "sex",
                "AnzahlFall": "new_confirmed",
                "AnzahlTodesfall": "new_deceased",
                "AnzahlGenesen": "new_recovered",
                "Meldedatum": "date",
                # "Datenstand": "date_updated",
                # "NeuerFall": "type_confirmed",
                # "NeuerTodesfall": "type_deceased",
                # "NeuGenesen": "type_recovered",
                # "Refdatum": "date",
                # "IstErkrankungsbeginn": "",
                # "Altersgruppe2": "",
            },
            drop=True,
            remove_regex=r"[^0-9a-z\s]",
        )

        # Translate sex labels; only male, female and unknown are given
        sex_adapter = lambda x: {"M": "male", "W": "female"}.get(x, "sex_unknown")
        data["sex"] = data["sex"].apply(sex_adapter)

        # Normalize age group labels
        data["age"] = data["age"].apply(_parse_age_bin)

        # Use proper ISO codes for the subregion1 level
        data["subregion1_code"] = data["subregion1_code"].apply(_SUBREGION1_CODE_MAP.get)

        # Make sure the subregion2 codes are strings
        data["subregion2_code"] = data["subregion2_code"].apply(
            lambda x: numeric_code_as_string(x, 5)
        )

        # Convert date to ISO format
        data["date"] = data["date"].astype(str).apply(lambda x: x[:10].replace("/", "-"))

        # Aggregate state-level data by adding all municipalities
        state = (
            data.drop(columns=["subregion2_code"])
            .groupby(["date", "subregion1_code", "age", "sex"])
            .sum()
            .reset_index()
        )
        state["key"] = "DE_" + state["subregion1_code"]

        # Aggregate country-level data by adding all states
        country = (
            data.drop(columns=["subregion1_code", "subregion2_code"])
            .groupby(["date", "age", "sex"])
            .sum()
            .reset_index()
        )
        country["key"] = "DE"

        # We can build the key for the data directly from the subregion codes
        data["key"] = "DE_" + data["subregion1_code"] + "_" + data["subregion2_code"]

        # Drop bogus records from the data
        state.dropna(subset=["subregion1_code"], inplace=True)
        data.dropna(subset=["subregion1_code", "subregion2_code"], inplace=True)

        return concat([country, state, data])
