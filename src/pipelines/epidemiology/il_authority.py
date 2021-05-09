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

import json
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List

import requests
from pandas import DataFrame, concat

from lib.cast import numeric_code_as_string, safe_int_cast
from lib.data_source import DataSource
from lib.net import get_retry
from lib.utils import table_rename


def _download_from_api(
    url: str, offset: int = 0, log_func: Callable[[str], None] = None
) -> List[Dict[str, Any]]:
    """
    Recursively download all records from data source's API respecting the maximum record
    transfer per request.
    """
    url_tpl = url + "&offset={offset}&limit=10000"
    url_fmt = url_tpl.format(offset=offset)
    get_opts = dict(timeout=60)

    try:
        res = get_retry(url_fmt, max_retries=10, **get_opts)
        res = res.json().get("result")
    except Exception as exc:
        if log_func:
            log_func(res.text if res else "Unknown error")
        raise exc

    rows = res.get("records", [])
    if len(rows) == 0:
        return rows
    else:
        return rows + _download_from_api(url, offset=offset + len(rows))


class IsraelDataSource(DataSource):
    def fetch(
        self,
        output_folder: Path,
        cache: Dict[str, str],
        fetch_opts: List[Dict[str, Any]],
        skip_existing: bool = False,
    ) -> Dict[str, str]:

        # Base URL comes from fetch_opts
        url_base = fetch_opts[0]["url"]

        # Create a deterministic file name
        file_path = (
            output_folder
            / "snapshot"
            / ("%s.%s" % (uuid.uuid5(uuid.NAMESPACE_DNS, url_base), "json"))
        )

        # Avoid download if the file exists and flag is set
        if not skip_existing or not file_path.exists():
            with open(file_path, "w") as fd:
                json.dump(_download_from_api(url_base), fd)

        return {0: str(file_path.absolute())}

    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename appropriate columns
        data = table_rename(
            dataframes[0],
            {
                "town_code": "subregion2_code",
                "date": "date",
                "accumulated_tested": "total_tested",
                "new_tested_on_date": "_new_tested_flag",
                "accumulated_cases": "total_confirmed",
                "new_cases_on_date": "_new_confirmed_flag",
                "accumulated_recoveries": "total_recovered",
                "new_recoveries_on_date": "_new_recovered_flag",
                "accumulated_hospitalized": "total_hospitalized",
                "new_hospitalized_on_date": "_new_hospitalized_flag",
                "accumulated_deaths": "total_deceased",
                "new_deaths_on_date": "_new_deceased_flag",
                "accumulated_vaccination_first_dose": "total_persons_vaccinated",
                "accumulated_vaccination_second_dose": "total_persons_fully_vaccinated",
                "town": "match_string",
            },
            drop=True,
        )

        # Convert date to ISO format and sort the data
        data["date"] = data["date"].astype(str).str.slice(0, 10)
        data.sort_values("date", inplace=True)

        # Because low counts are masked, we assume <15 = 1 as a rough estimate
        for statistic in (
            "confirmed",
            "deceased",
            "tested",
            "recovered",
            "hospitalized",
            "persons_vaccinated",
            "persons_fully_vaccinated",
        ):
            col = f"total_{statistic}"
            if col in data.columns:
                low_count_mask = data[col] == "<15"
                data.loc[low_count_mask, col] = 1
                # We can fill the data with zeroes since every case should be recorded by source
                data[col] = data[col].apply(safe_int_cast).fillna(0)

        # Estimate total vaccine doses administered from first and second dose counts
        data["total_vaccine_doses_administered"] = (
            data["total_persons_vaccinated"] + data["total_persons_fully_vaccinated"]
        )
        # Properly format the region code and group by it
        data["subregion2_code"] = data["subregion2_code"].apply(
            lambda x: numeric_code_as_string(x, 4)
        )
        data = data.groupby(["date", "subregion2_code", "match_string"]).sum().reset_index()

        # Aggregate to country level and drop unknown locations
        data["country_code"] = "IL"
        intra_country_columns = ["subregion2_code", "match_string"]
        country = data.drop(columns=intra_country_columns)
        country = data.groupby("country_code").sum().reset_index()
        data.dropna(subset=intra_country_columns, inplace=True)

        # Drop country-level confirmed and deceased since we have better sources of aggregated data
        country["key"] = country["country_code"]
        country.drop(columns=["total_confirmed", "total_deceased"], inplace=True)

        # Get the admin level 1 and key from metadata
        il = aux["metadata"][["key", "country_code", "subregion1_code", "subregion2_code"]]
        il = il[(il["country_code"] == "IL") & il["subregion2_code"].notna()]
        il["subregion2_code"] = il["subregion2_code"].apply(lambda x: numeric_code_as_string(x, 4))
        data = data.merge(il, how="left")

        # Aggregate by admin level 1
        admin_l1 = data.groupby(["date", "country_code", "subregion1_code"]).sum().reset_index()
        admin_l1["key"] = admin_l1["country_code"] + "_" + admin_l1["subregion1_code"]

        return concat([country, admin_l1, data])
