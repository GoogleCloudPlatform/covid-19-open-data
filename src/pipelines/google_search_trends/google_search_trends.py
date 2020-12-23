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
from pandas import DataFrame, concat
from lib.concurrent import thread_map
from lib.data_source import DataSource


def _normalize_column_name(column: str) -> str:
    column = column.lower()
    column = column.replace("symptom:", "")
    column = column.replace("–", "_")
    column = column.replace(" ", "_")
    column = column.replace("-", "_")
    column = column.replace("'", "")
    return column


def _rename_columns(data: DataFrame) -> DataFrame:
    column_adapter = {
        "date": "date",
        "country_region_code": "country_code",
        "sub_region_1": "subregion1_name",
        "sub_region_2": "subregion2_name",
        "sub_region_1_code": "subregion1_code",
        "sub_region_2_code": "subregion2_code",
    }
    data = data.rename(columns=column_adapter)
    data.columns = [
        col if col in column_adapter.values() else "search_trends_" + _normalize_column_name(col)
        for col in data.columns
    ]

    return data


def _parse_subregion_code(code: str) -> str:
    return code.split("-")[-1]


def _process_chunk(data: DataFrame) -> DataFrame:
    data = _rename_columns(data)

    # Keep only regions which are not metro areas
    if "metro_area" in data.columns:
        data = data[data["metro_area"].isna()]

    # We can derive the keys directly for all data
    data["key"] = None
    subregion1_isna_mask = data.subregion1_code.isna()
    subregion2_isna_mask = data.subregion2_code.isna()

    # Country level has null subregions 1 and 2
    country_level_mask = subregion1_isna_mask & subregion2_isna_mask
    data.loc[country_level_mask, "key"] = data.loc[country_level_mask, "country_code"]

    state_level_mask = ~subregion1_isna_mask & subregion2_isna_mask
    data.loc[state_level_mask, "key"] = (
        data.loc[state_level_mask, "country_code"]
        + "_"
        + data.loc[state_level_mask, "subregion1_code"].apply(_parse_subregion_code)
    )

    county_level_mask = ~subregion1_isna_mask & ~subregion2_isna_mask
    data.loc[county_level_mask, "key"] = (
        data.loc[county_level_mask, "country_code"]
        + "_"
        + data.loc[county_level_mask, "subregion1_code"].apply(_parse_subregion_code)
        + "_"
        + data.loc[county_level_mask, "subregion2_code"].apply(_parse_subregion_code)
    )

    return data


class GoogleSearchTrendsDataSource(DataSource):
    def fetch(
        self,
        output_folder: Path,
        cache: Dict[str, str],
        fetch_opts: List[Dict[str, Any]],
        skip_existing: bool = False,
    ) -> Dict[str, str]:
        base_opts = dict(fetch_opts[0])
        url_tpl: str = base_opts.pop("url")

        # Get the files to process from the `parse` config options
        country_codes: List[str] = self.config["parse"]["country_codes"]
        subregion_names: List[str] = self.config["parse"].get("subregion_names")

        # Generate the URLs for the files to download from the template
        url_list = {}
        for country_code in country_codes:
            if not subregion_names:
                key = country_code
                url_list[key] = url_tpl.format(level="country", region_code=key)
            else:
                for subregion_name in subregion_names:
                    key = f"{country_code}_{subregion_name.replace(' ', '_')}"
                    url_list[key] = url_tpl.format(level="sub_region_1", region_code=key)

        # Replace the fetch options with our own processed options
        fetch_opts = [{"name": key, "url": url, **base_opts} for key, url in url_list.items()]
        return super().fetch(output_folder, cache, fetch_opts, skip_existing=skip_existing)

    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        google_keys = aux["google_key_map"].set_index("google_location_key")["key"].to_dict()
        data = concat(thread_map(_process_chunk, dataframes.values(), total=len(dataframes)))
        data[["key"]].drop_duplicates().to_csv("google_keys.csv", index=False)
        data["key"] = data["key"].apply(lambda x: google_keys.get(x, x))
        return data.dropna(subset=["key"])
