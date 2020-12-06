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

import datetime
from functools import partial
from pathlib import Path
from typing import Any, Dict, List

import requests
from pandas import DataFrame, concat, isna

from lib.case_line import convert_cases_to_time_series
from lib.cast import safe_int_cast, numeric_code_as_string
from lib.error_logger import ErrorLogger
from lib.concurrent import process_map, thread_map
from lib.net import download_snapshot
from lib.pipeline import DataSource
from lib.time import datetime_isoformat
from lib.utils import table_rename

_IBGE_STATES = {
    # Norte
    11: "RO",
    12: "AC",
    13: "AM",
    14: "RR",
    15: "PA",
    16: "AP",
    17: "TO",
    # Nordeste
    21: "MA",
    22: "PI",
    23: "CE",
    24: "RN",
    25: "PB",
    26: "PE",
    27: "AL",
    28: "SE",
    29: "BA",
    # Sudeste
    31: "MG",
    32: "ES",
    33: "RJ",
    35: "SP",
    # Sul
    41: "PR",
    42: "SC",
    43: "RS",
    # Centro-Oeste
    50: "MS",
    51: "MT",
    52: "GO",
    53: "DF",
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


def _download_open_data(
    logger: ErrorLogger,
    url_tpl: str,
    output_folder: Path,
    ibge_code: str,
    max_volumes: int = 12,
    **download_opts,
) -> Dict[str, str]:
    logger.log_debug(f"Downloading Brazil data for {ibge_code}...")

    # Since we are guessing the URL, we forgive errors in the download
    output = {}
    download_opts = dict(download_opts, ignore_failure=True)
    map_func = partial(download_snapshot, output_folder=output_folder, **download_opts)
    map_iter = [url_tpl.format(f"{ibge_code}-{idx + 1}") for idx in range(max_volumes)]
    for idx, file_path in enumerate(thread_map(map_func, map_iter)):
        if file_path is not None:
            output[f"{ibge_code}-{idx + 1}"] = file_path

    # Filter out empty files, which can happen if download fails in an unexpected way
    output = {name: path for name, path in output.items() if Path(path).stat().st_size > 0}

    # If the output is not split into volumes, fall back to single file URL
    if output:
        return output
    else:
        url = url_tpl.format(ibge_code)
        return {ibge_code: download_snapshot(url, output_folder, **download_opts)}


def _process_partition(cases: DataFrame) -> DataFrame:
    cases = cases.copy()

    # Confirmed cases are only those with a confirmed positive test result
    cases["date_new_confirmed"] = None
    confirmed_mask = cases["_test_result"] == "Positivo"
    cases.loc[confirmed_mask, "date_new_confirmed"] = cases.loc[confirmed_mask, "date_new_tested"]

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

    # Make sure our region codes are of type str
    cases["subregion2_code"] = cases["subregion2_code"].apply(safe_int_cast)
    # The last digit of the region code is actually not necessary
    cases["subregion2_code"] = cases["subregion2_code"].apply(
        lambda x: None if isna(x) else str(int(x))[:-1]
    )

    # Convert ages to int, and translate sex (no "other" sex/gender reported)
    cases["age"] = cases["age"].apply(safe_int_cast)
    cases["sex"] = cases["sex"].str.lower().apply({"masculino": "male", "feminino": "female"}.get)

    # Convert to time series format
    data = convert_cases_to_time_series(cases, index_columns=["subregion1_code", "subregion2_code"])

    # Convert date to ISO format
    data["date"] = data["date"].str.slice(0, 10)
    data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%Y-%m-%d"))

    # Get rid of bogus records
    data = data.dropna(subset=["date"])
    data = data[data["date"] >= "2020-01-01"]
    data = data[data["date"] < str(datetime.date.today() + datetime.timedelta(days=2))]

    # Aggregate data by state
    state = (
        data.drop(columns=["subregion2_code"])
        .groupby(["date", "subregion1_code", "age", "sex"])
        .sum()
        .reset_index()
    )
    state["key"] = "BR_" + state["subregion1_code"]

    # We can derive the key from subregion1 + subregion2
    data = data[data["subregion2_code"].notna() & (data["subregion2_code"] != "")]
    data["key"] = "BR_" + data["subregion1_code"] + "_" + data["subregion2_code"]

    return concat([state, data])


class BrazilStratifiedDataSource(DataSource):
    def fetch(
        self,
        output_folder: Path,
        cache: Dict[str, str],
        fetch_opts: List[Dict[str, Any]],
        skip_existing: bool = False,
    ) -> Dict[str, str]:

        output = {}
        download_options = dict(fetch_opts[0], skip_existing=skip_existing)
        url_tpl = download_options.pop("url")
        map_opts = dict(desc="Downloading Brazil Open Data")
        map_iter = [code.lower() for code in _IBGE_STATES.values()]
        map_func = partial(_download_open_data, self, url_tpl, output_folder, **download_options)
        for partial_output in thread_map(map_func, map_iter, **map_opts):
            output.update(partial_output)

        return output

    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        # Manipulate the parse options here because we have access to the columns adapter and we
        # can then limit the columns being read to save space.
        parse_opts = {
            **dict(parse_opts),
            "error_bad_lines": False,
            "usecols": _column_adapter.keys(),
        }
        return super().parse(sources, aux, **parse_opts)

    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Partition dataframes based on the state the data is for
        partitions = {code: [] for code in _IBGE_STATES.values()}
        for df in dataframes.values():
            df = table_rename(df, _column_adapter, drop=True)
            apply_func = lambda x: _IBGE_STATES.get(safe_int_cast(x))
            df["subregion1_code"] = df["_state_code"].apply(apply_func)
            for code, group in df.groupby("subregion1_code"):
                partitions[code].append(group)

        # Process each partition in separate threads
        map_opts = dict(desc="Processing Partitions", total=len(partitions))
        map_iter = (concat(chunks) for chunks in partitions.values())
        return concat(process_map(_process_partition, map_iter, **map_opts))
