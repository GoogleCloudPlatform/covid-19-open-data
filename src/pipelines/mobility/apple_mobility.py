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

import re
import numpy
import requests
from pathlib import Path
from typing import Any, Dict, List
from pandas import DataFrame, isnull
from lib.pipeline import DataSource


_url_base = "https://covid19-static.cdn-apple.com"


class AppleMobilityDataSource(DataSource):
    def fetch(
        self, output_folder: Path, cache: Dict[str, str], fetch_opts: List[Dict[str, Any]]
    ) -> List[str]:
        api_res = requests.get(fetch_opts[0]["url"]).json()
        fetch_opts = [
            {"url": f"{_url_base}{api_res['basePath']}{api_res['regions']['en-us']['csvPath']}"}
        ]
        return super().fetch(output_folder, cache, fetch_opts)

    @staticmethod
    def process_record(record: Dict):
        subregion1 = record["subregion1_name"]
        subregion2 = record["subregion2_name"]

        # Early exit: country-level data
        if isnull(subregion1):
            return None

        if isnull(subregion2):
            match_string = subregion1
        else:
            match_string = subregion2

        match_string = re.sub(r"\(.+\)", "", match_string).split("/")[0]
        for token in (
            "Province",
            "Prefecture",
            "State of",
            "Canton of",
            "Autonomous",
            "Voivodeship",
            "District",
        ):
            match_string = match_string.replace(token, "")

        # Workaround for "Blekinge County"
        if record["country_code"] != "US":
            match_string = match_string.replace("County", "")

        return match_string.strip()

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = dataframes[0]
        data = data[data.geo_type != "city"].copy()

        # Convert into more intuitive country-subregion1-subregion2 format
        country_level_mask = data.geo_type == "country/region"
        subregion1_level_mask = data.geo_type == "sub-region"
        subregion2_level_mask = ~(country_level_mask | subregion1_level_mask)
        data.loc[country_level_mask, "country_name"] = data.loc[country_level_mask, "region"]
        data.loc[subregion1_level_mask, "subregion1_name"] = data.loc[
            subregion1_level_mask, "region"
        ]
        data.loc[subregion2_level_mask, "subregion2_name"] = data.loc[
            subregion2_level_mask, "region"
        ]
        data.loc[subregion2_level_mask, "subregion1_name"] = data.loc[
            subregion2_level_mask, "sub-region"
        ]
        data.loc[subregion1_level_mask, "country_name"] = data.loc[subregion1_level_mask, "country"]
        data.loc[subregion2_level_mask, "country_name"] = data.loc[subregion2_level_mask, "country"]

        # Correct name for USA, as per ISO standard
        data.loc[data.country_name == "United States", "country_name"] = "United States of America"

        # "_nan_magic_number" replacement necessary to work around
        # https://github.com/pandas-dev/pandas/issues/3729
        # This issue will be fixed in Pandas 1.1
        _nan_magic_number = -123456789
        data.fillna(_nan_magic_number, inplace=True)

        # data.loc[null_country_mask, "country"] = data.loc[null_country_mask, "region"]
        data.drop(
            columns=["geo_type", "country", "region", "sub-region", "alternative_name"],
            inplace=True,
        )
        data = (
            data.melt(
                id_vars=[
                    "country_name",
                    "subregion1_name",
                    "subregion2_name",
                    "transportation_type",
                ],
                var_name="date",
            )
            .pivot_table(
                index=["date", "country_name", "subregion1_name", "subregion2_name"],
                columns="transportation_type",
            )
            .reset_index()
        )
        data.columns = [col2 + ("" if col1 == "value" else col1) for col1, col2 in data.columns]
        data.replace([_nan_magic_number], numpy.nan, inplace=True)

        meta = aux["metadata"]
        data = data.merge(meta[meta.subregion1_code.isna()][["country_code", "country_name"]])
        data["match_string"] = data.apply(AppleMobilityDataSource.process_record, axis=1)

        # We can derive the key directly from country code for country-level data
        data["key"] = None
        country_level_mask = data.match_string.isna()
        data.loc[country_level_mask, "key"] = data.loc[country_level_mask, "country_code"]

        # Drop intra-country records for which we don't have regional data
        regional_data_countries = meta.loc[~meta.subregion1_code.isna(), "country_code"].unique()
        data = data[~data.key.isna() | data.country_code.isin(regional_data_countries)]

        # Instead of subregion name, always use match_string
        data.loc[~data.subregion2_name.isna(), "subregion2_name"] = ""

        # For non-USA records, we don't need the subregion names, we just use match_string
        usa_mask = data.country_code == "US"
        data.loc[~usa_mask, "subregion1_name"] = ""

        # Convert the units to match Google's mobility report
        value_columns = ["driving", "transit", "walking"]
        data[value_columns] = data[value_columns] - 100

        # Add column prefix and return
        return data.rename(columns={col: f"mobility_{col}" for col in value_columns})
