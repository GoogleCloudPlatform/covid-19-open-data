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

from pathlib import Path
from typing import Any, Dict, List

import requests
from pandas import DataFrame, concat, isna

from lib.case_line import convert_cases_to_time_series
from lib.cast import safe_int_cast, numeric_code_as_string
from lib.pipeline import DataSource
from lib.time import datetime_isoformat
from lib.utils import table_rename

_IBGE_STATES = {
    # Norte
    "RO": 11,
    "AC": 12,
    "AM": 13,
    "RR": 14,
    "PA": 15,
    "AP": 16,
    "TO": 17,
    # Nordeste
    "MA": 21,
    "PI": 22,
    "CE": 23,
    "RN": 24,
    "PB": 25,
    "PE": 26,
    "AL": 27,
    "SE": 28,
    "BA": 29,
    # Sudeste
    "MG": 31,
    "ES": 32,
    "RJ": 33,
    "SP": 35,
    # Sul
    "PR": 41,
    "SC": 42,
    "RS": 43,
    # Centro-Oeste
    "MS": 50,
    "MT": 51,
    "GO": 52,
    "DF": 53,
}


class BrazilMunicipalitiesDataSource(DataSource):
    def fetch(
        self, output_folder: Path, cache: Dict[str, str], fetch_opts: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        # Get the URL from a fake browser request
        url = requests.get(
            "https://xx9p7hp1p7.execute-api.us-east-1.amazonaws.com/prod/PortalGeral",
            headers={
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-GB,en;q=0.5",
                "X-Parse-Application-Id": "unAFkcaNDeXajurGB7LChj8SgQYS2ptm",
                "Origin": "https://covid.saude.gov.br",
                "Connection": "keep-alive",
                "Referer": "https://covid.saude.gov.br/",
                "Pragma": "no-cache",
                "Cache-Control": "no-cache",
                "TE": "Trailers",
            },
        ).json()["results"][0]["arquivo"]["url"]

        # Pass the actual URL down to fetch it
        return super().fetch(output_folder, cache, [{"url": url}])

    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = table_rename(
            dataframes[0],
            {
                "data": "date",
                "estado": "subregion1_code",
                "codmun": "subregion2_code",
                "municipio": "subregion2_name",
                "casosNovos": "new_confirmed",
                "obitosNovos": "new_deceased",
                "casosAcumulado": "total_confirmed",
                "obitosAcumulado": "total_deceased",
                "Recuperadosnovos": "new_recovered",
            },
            drop=True,
        )

        # Convert date to ISO format
        data["date"] = data["date"].astype(str)

        # Parse region codes as strings
        data["subregion2_code"] = data["subregion2_code"].apply(
            lambda x: numeric_code_as_string(x, 6)
        )

        # Country-level data has null state
        data["key"] = None
        country_mask = data["subregion1_code"].isna()
        data.loc[country_mask, "key"] = "BR"

        # State-level data has null municipality
        state_mask = data["subregion2_code"].isna()
        data.loc[~country_mask & state_mask, "key"] = "BR_" + data["subregion1_code"]

        # We can derive the key from subregion1 + subregion2
        data.loc[~country_mask & ~state_mask, "key"] = (
            "BR_" + data["subregion1_code"] + "_" + data["subregion2_code"]
        )

        # Drop bogus data
        data = data[data["subregion2_code"].str.slice(-4) != "0000"]

        return data


_column_adapter = {
    "sexo": "sex",
    "idade": "age",
    "municipioIBGE": "subregion2_code",
    "dataTeste": "date_new_tested",
    "dataInicioSintomas": "_date_onset",
    "estadoIBGE": "_state_code",
    "evolucaoCaso": "_prognosis",
    "dataEncerramento": "_date_update",
    "resultadoTeste": "_test_result",
    "classificacaoFinal": "_classification",
}


class BrazilStratifiedDataSource(DataSource):
    def fetch(
        self, output_folder: Path, cache: Dict[str, str], fetch_opts: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        # The source URL is a template which we must format for the requested state
        parse_opts = self.config["parse"]
        fetch_opts = [
            {**opts, "url": opts["url"].format(parse_opts["subregion1_code"].lower())}
            for opts in fetch_opts
        ]
        return super().fetch(output_folder, cache, fetch_opts)

    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        # Manipulate the parse options here because we have access to the columns adapter
        parse_opts = {**parse_opts, "error_bad_lines": False, "usecols": _column_adapter.keys()}
        return super().parse(sources, aux, **parse_opts)

    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        cases = table_rename(dataframes[0], _column_adapter, drop=True)

        # Keep only cases for a single state
        subregion1_code = parse_opts["subregion1_code"]
        cases = cases[cases["_state_code"].apply(safe_int_cast) == _IBGE_STATES[subregion1_code]]

        # Confirmed cases are only those with a confirmed positive test result
        cases["date_new_confirmed"] = None
        confirmed_mask = cases["_test_result"] == "Positivo"
        cases.loc[confirmed_mask, "date_new_confirmed"] = cases.loc[
            confirmed_mask, "date_new_tested"
        ]

        # Deceased cases have a specific label and the date is the "closing" date
        cases["date_new_deceased"] = None
        deceased_mask = cases["_prognosis"] == "Óbito"
        cases.loc[confirmed_mask, "date_new_deceased"] = cases.loc[deceased_mask, "_date_update"]

        # Recovered cases have a specific label and the date is the "closing" date
        cases["date_new_recovered"] = None
        recovered_mask = cases["_prognosis"] == "Cured"
        cases.loc[confirmed_mask, "date_new_recovered"] = cases.loc[recovered_mask, "_date_update"]

        # Drop columns which we have no use for
        cases = cases[[col for col in cases.columns if not col.startswith("_")]]

        # Subregion code comes from the parsing parameters
        cases["subregion1_code"] = subregion1_code

        # Make sure our region code is of type str
        cases["subregion2_code"] = cases["subregion2_code"].apply(safe_int_cast)
        # The last digit of the region code is actually not necessary
        cases["subregion2_code"] = cases["subregion2_code"].apply(
            lambda x: None if isna(x) else str(int(x))[:-1]
        )

        # Drop bogus records from the data
        cases = cases[~cases["subregion1_code"].isna() & (cases["subregion1_code"] != "")]
        cases = cases[~cases["subregion2_code"].isna() & (cases["subregion2_code"] != "")]

        # We can derive the key from subregion1 + subregion2
        cases["key"] = "BR_" + cases["subregion1_code"] + "_" + cases["subregion2_code"]

        # Convert ages to int, and translate sex (no "other" sex/gender reported)
        cases["age"] = cases["age"].apply(safe_int_cast)
        cases["sex"] = (
            cases["sex"].str.lower().apply({"masculino": "male", "femenino": "female"}.get)
        )

        # Convert to time series format
        data = convert_cases_to_time_series(cases, index_columns=["key"])

        # Convert date to ISO format
        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%Y-%m-%dT%H:%M:%S.%fZ"))

        # Aggregate for the whole state
        state = data.drop(columns=["key"]).groupby(["date", "age", "sex"]).sum().reset_index()
        state["key"] = f"BR_{subregion1_code}"

        return concat([data, state])
