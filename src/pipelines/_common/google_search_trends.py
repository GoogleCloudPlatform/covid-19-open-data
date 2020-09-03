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
from lib.cast import numeric_code_as_string
from lib.concurrent import thread_map
from lib.data_source import DataSource
from lib.io import read_file


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
    data["subregion1_code"] = data["subregion1_code"].apply(
        lambda x: x.split("-")[-1] if x else None
    )
    data["subregion2_code"] = data["subregion2_code"].apply(lambda x: numeric_code_as_string(x, 5))

    data.columns = [
        col
        if col in column_adapter.values()
        else f"search_trends_{col.replace('symptom:', '').replace(' ', '').lower()}"
        for col in data.columns
    ]

    return data


class GoogleSearchTrendsL1DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = _rename_columns(concat(dataframes.values()))

        # Keep only regions which are not metro areas
        if "metro_area" in data.columns:
            data = data[data["metro_area"].isna()]

        # We can derive the keys directly for all data, because it's US-only
        data["key"] = None

        country_level_mask = data.subregion1_code.isna() & data.subregion2_code.isna()
        data.loc[country_level_mask, "key"] = data.loc[country_level_mask, "country_code"]

        state_level_mask = ~data.subregion1_code.isna() & data.subregion2_code.isna()
        data.loc[state_level_mask, "key"] = (
            data.loc[state_level_mask, "country_code"]
            + "_"
            + data.loc[state_level_mask, "subregion1_code"]
        )

        county_level_mask = ~data.subregion1_code.isna() & ~data.subregion2_code.isna()
        data.loc[county_level_mask, "key"] = (
            data.loc[county_level_mask, "country_code"]
            + "_"
            + data.loc[county_level_mask, "subregion1_code"]
            + "_"
            + data.loc[county_level_mask, "subregion2_code"]
        )

        return data


class GoogleSearchTrendsL2DataSource(GoogleSearchTrendsL1DataSource):
    def fetch(
        self, output_folder: Path, cache: Dict[str, str], fetch_opts: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        # Data file is too big to store in Git, so pass-through the URL to parse manually
        return {
            source.get("name", idx): {"url": source["url"]} for idx, source in enumerate(fetch_opts)
        }

    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts):
        url_tpl = sources[0]["url"]

        # Some states cannot be found in the dataset
        states_banlist = [
            "American Samoa",
            "District of Columbia",
            "Guam",
            "Northern Mariana Islands",
            "Puerto Rico",
            "Virgin Islands",
        ]

        states = aux["metadata"]
        states = states.loc[states["country_code"] == "US", "subregion1_name"].dropna().unique()
        states = [state for state in states if state not in states_banlist]
        states_url = [
            url_tpl.format(
                subregion1_name_path=state_name.replace(" ", "%20"),
                subregion1_name_file=state_name.replace(" ", "_"),
            )
            for state_name in states
        ]
        dataframes = {idx: df for idx, df in enumerate(thread_map(read_file, states_url))}
        return self.parse_dataframes(dataframes, aux, **parse_opts)
