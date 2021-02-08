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

from functools import partial
from pathlib import Path
from typing import Any, Dict, List

from pandas import DataFrame, concat
from lib.cast import age_group, safe_int_cast
from lib.data_source import DataSource
from lib.io import read_file
from lib.constants import SRC
from lib.concurrent import thread_map
from lib.time import datetime_isoformat
from lib.utils import table_rename


_column_adapter = {
    "key": "key",
    "date": "date",
    "casConfirmes": "total_confirmed",
    "deces": "total_deceased",
    "testsPositifs": "new_confirmed",
    "testsRealises": "new_tested",
    "gueris": "new_recovered",
    "hospitalises": "current_hospitalized",
    "reanimation": "current_intensive_care",
}


def _get_region(
    url_tpl: str, column_adapter: Dict[str, str], iso_map: Dict[str, str], subregion1_code: str
):
    code = iso_map[subregion1_code]
    data = read_file(url_tpl.format(code))
    data["key"] = f"FR_{subregion1_code}"
    return table_rename(data, column_adapter, drop=True)


def _get_department(url_tpl: str, column_adapter: Dict[str, str], record: Dict[str, str]):
    subregion1_code = record["subregion1_code"]
    subregion2_code = record["subregion2_code"]
    code = f"DEP-{subregion2_code}"
    data = read_file(url_tpl.format(code))
    data["key"] = f"FR_{subregion1_code}_{subregion2_code}"
    return table_rename(data, column_adapter, drop=True)


def _get_country(url_tpl: str, column_adapter: Dict[str, str]):
    data = read_file(url_tpl.format("FRA"))
    data["key"] = "FR"
    return table_rename(data, column_adapter, drop=True)


class FranceDataSource(DataSource):
    def fetch(
        self,
        output_folder: Path,
        cache: Dict[str, str],
        fetch_opts: List[Dict[str, Any]],
        skip_existing: bool = False,
    ) -> Dict[str, str]:
        # URL is just a template, so pass-through the URL to parse manually
        return {idx: source["url"] for idx, source in enumerate(fetch_opts)}

    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        url_tpl = sources[0]
        metadata = aux["metadata"]
        metadata = metadata[metadata["country_code"] == "FR"]

        fr_isos = read_file(SRC / "data" / "fr_iso_codes.csv")
        fr_iso_map = {iso: code for iso, code in zip(fr_isos["iso_code"], fr_isos["region_code"])}
        fr_codes = metadata[["subregion1_code", "subregion2_code"]].dropna()
        regions_iter = fr_codes["subregion1_code"].unique()
        deps_iter = [record for _, record in fr_codes.iterrows()]

        # For country level, there is no need to estimate confirmed from tests
        column_adapter_country = dict(_column_adapter)
        column_adapter_country.pop("testsPositifs")

        # Get country level data
        country = _get_country(url_tpl, column_adapter_country)

        # Country level data has totals instead of diffs, so we compute the diffs by hand
        country.sort_values("date", inplace=True)
        country["new_confirmed"] = country["total_confirmed"].diff()
        country.drop(columns=["total_confirmed"], inplace=True)

        # For region level, we can only estimate confirmed from tests
        column_adapter_region = dict(_column_adapter)
        column_adapter_region.pop("casConfirmes")

        # Get region level data
        get_region_func = partial(_get_region, url_tpl, column_adapter_region, fr_iso_map)
        regions = concat(list(thread_map(get_region_func, regions_iter)))

        # Get department level data
        get_department_func = partial(_get_department, url_tpl, column_adapter_region)
        departments = concat(list(thread_map(get_department_func, deps_iter)))

        data = concat([country, regions, departments])
        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%Y-%m-%d %H:%M:%S"))
        return data.sort_values("date")


class FranceStratifiedDataSource(FranceDataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        url_tpl = sources[0]
        metadata = aux["metadata"]
        metadata = metadata[metadata["country_code"] == "FR"]

        fr_isos = read_file(SRC / "data" / "fr_iso_codes.csv")
        fr_iso_map = {iso: code for iso, code in zip(fr_isos["iso_code"], fr_isos["region_code"])}
        fr_codes = metadata[["subregion1_code", "subregion2_code"]].dropna()
        regions_iter = fr_codes["subregion1_code"].unique()
        deps_iter = [record for _, record in fr_codes.iterrows()]

        column_adapter = {
            "key": "key",
            "date": "date",
            "testsRealisesDetails": "_breakdown_tested",
            "testsPositifsDetails": "_breakdown_confirmed",
        }

        # Get country level data
        country = _get_country(url_tpl, column_adapter)

        # Get region level data
        get_region_func = partial(_get_region, url_tpl, column_adapter, fr_iso_map)
        regions = concat(list(thread_map(get_region_func, regions_iter)))

        # Get department level data
        get_department_func = partial(_get_department, url_tpl, column_adapter)
        departments = concat(list(thread_map(get_department_func, deps_iter)))

        data = concat([country, regions, departments])
        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%Y-%m-%d %H:%M:%S"))

        data["_breakdown_tested"].fillna("", inplace=True)
        data["_breakdown_confirmed"].fillna("", inplace=True)

        records = {"confirmed": [], "tested": []}
        for key, row in data.set_index("key").iterrows():
            for statistic in records.keys():
                if row[f"_breakdown_{statistic}"] != "":
                    for item in row[f"_breakdown_{statistic}"]:
                        records[statistic].append(
                            {
                                "key": key,
                                "date": row["date"],
                                "age": item["age"],
                                "sex": item.get("sexe"),
                                f"new_{statistic}": item["value"],
                            }
                        )

        df1 = DataFrame.from_records(records["tested"])
        df2 = DataFrame.from_records(records["confirmed"])
        data = df1.merge(df2, how="outer")

        data = data[~data["age"].isin(["0", "A", "B", "C", "D", "E"])]
        data["age"] = data["age"].apply(lambda x: age_group(safe_int_cast(x)))

        sex_adapter = lambda x: {"h": "male", "f": "female"}.get(x, "sex_unknown")
        data["sex"] = data["sex"].apply(sex_adapter)
        return data
