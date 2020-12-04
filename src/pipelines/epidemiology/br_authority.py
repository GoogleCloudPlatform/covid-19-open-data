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
from lib.net import download_snapshot
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
        self,
        output_folder: Path,
        cache: Dict[str, str],
        fetch_opts: List[Dict[str, Any]],
        skip_existing: bool = False,
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
            timeout=60,
        ).json()["results"][0]["arquivo"]["url"]

        # Pass the actual URL down to fetch it
        return super().fetch(output_folder, cache, [{"url": url}], skip_existing=skip_existing)

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
                "Recuperadosnovos": "total_recovered",
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
        self,
        output_folder: Path,
        cache: Dict[str, str],
        fetch_opts: List[Dict[str, Any]],
        skip_existing: bool = False,
    ) -> Dict[str, str]:
        # The source URL is a template which we must format for the requested state
        parse_opts = self.config["parse"]
        code = parse_opts["subregion1_code"].lower()

        # Some datasets are split into "volumes" so we try to guess the URL
        base_opts = dict(fetch_opts[0])
        url_tpl = base_opts.pop("url")
        fetch_opts = [{"url": url_tpl.format(f"{code}-{idx}"), **base_opts} for idx in range(1, 10)]

        # Since we are guessing the URL, we forgive errors in the download
        output = {}
        for idx, source_config in enumerate(fetch_opts):
            url = source_config["url"]
            name = source_config.get("name", idx)
            download_opts = source_config.get("opts", {})
            try:
                self.log_debug(f"Downloading {url}...")
                output[name] = download_snapshot(
                    url, output_folder, skip_existing=skip_existing, **download_opts
                )
            except:
                self.log_warning(f"Failed to download URL {url}")
                break

        # Filter out empty files, which can happen if download fails in an unexpected way
        output = {name: path for name, path in output.items() if Path(path).stat().st_size > 0}

        # If the output is not split into volumes, fall back to single file URL
        if output:
            return output
        else:
            fetch_opts = [{"url": url_tpl.format(code), **base_opts}]
            return super().fetch(output_folder, cache, fetch_opts, skip_existing=skip_existing)

    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        # Manipulate the parse options here because we have access to the columns adapter
        parse_opts = {
            **dict(parse_opts),
            "error_bad_lines": False,
            "usecols": _column_adapter.keys(),
        }
        return super().parse(sources, aux, **parse_opts)

    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Subregion code comes from the parsing parameters
        subregion1_code = parse_opts["subregion1_code"]

        # Join all input data into a single table
        cases = table_rename(concat(dataframes.values()), _column_adapter, drop=True)

        # Keep only cases for a single state
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
        cases.loc[deceased_mask, "date_new_deceased"] = cases.loc[deceased_mask, "_date_update"]

        # Only count deceased cases from confirmed subjects
        cases.loc[~confirmed_mask, "date_new_deceased"] = None

        # Recovered cases have a specific label and the date is the "closing" date
        cases["date_new_recovered"] = None
        recovered_mask = cases["_prognosis"] == "Cured"
        cases.loc[recovered_mask, "date_new_recovered"] = cases.loc[recovered_mask, "_date_update"]

        # Only count recovered cases from confirmed subjects
        cases.loc[~confirmed_mask, "date_new_recovered"] = None

        # Drop columns which we have no use for
        cases = cases[[col for col in cases.columns if not col.startswith("_")]]

        # Make sure our region code is of type str
        cases["subregion2_code"] = cases["subregion2_code"].apply(safe_int_cast)
        # The last digit of the region code is actually not necessary
        cases["subregion2_code"] = cases["subregion2_code"].apply(
            lambda x: None if isna(x) else str(int(x))[:-1]
        )

        # Null and unknown records are state only
        subregion2_null_mask = cases["subregion2_code"].isna()
        cases.loc[subregion2_null_mask, "key"] = "BR_" + subregion1_code

        # We can derive the key from subregion1 + subregion2
        cases.loc[~subregion2_null_mask, "key"] = (
            "BR_" + subregion1_code + "_" + cases.loc[~subregion2_null_mask, "subregion2_code"]
        )

        # Convert ages to int, and translate sex (no "other" sex/gender reported)
        cases["age"] = cases["age"].apply(safe_int_cast)
        cases["sex"] = (
            cases["sex"]
            .str.lower()
            .apply({"masculino": "male", "feminino": "female", "indefinido": None}.get)
        )

        # Convert to time series format
        data = convert_cases_to_time_series(cases, index_columns=["key"])

        # Convert date to ISO format
        data["date"] = data["date"].str.slice(0, 10)
        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%Y-%m-%d"))

        # Get rid of bogus records
        data = data.dropna(subset=["date"])
        data = data[data["date"] >= "2020-01-01"]

        # Aggregate for the whole state
        state = data.drop(columns=["key"]).groupby(["date", "age", "sex"]).sum().reset_index()
        state["key"] = "BR_" + subregion1_code

        return concat([data, state])
