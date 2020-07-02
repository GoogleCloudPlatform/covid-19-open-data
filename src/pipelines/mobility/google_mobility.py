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
from typing import Any, Dict, List, Tuple
from pandas import DataFrame, Int64Dtype, merge, isna, concat
from lib.cast import safe_int_cast
from lib.pipeline import DataSource, DataSource, DataPipeline
from lib.time import datetime_isoformat
from lib.utils import ROOT


def _process_record(record: Dict):
    subregion1 = record["subregion1_name"]
    subregion2 = record["subregion2_name"]

    # Early exit: country-level data
    if isna(subregion1):
        return None

    if isna(subregion2):
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


class GoogleMobilityDataSource(DataSource):
    data_urls: List[str] = ["https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv"]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = data = dataframes[0].rename(
            columns={
                "country_region_code": "country_code",
                "sub_region_1": "subregion1_name",
                "sub_region_2": "subregion2_name",
            }
        )

        data.columns = [
            f"mobility_{re.sub('_percent.+', '', col)}" if "percent" in col else col
            for col in data.columns
        ]

        # We can derive the key directly from country code for country-level data
        data["key"] = None
        country_level_mask = data.subregion1_name.isna() & data.subregion2_name.isna()
        data.loc[country_level_mask, "key"] = data.loc[country_level_mask, "country_code"]

        # Guadeloupe is considered a subregion of France for reporting purposes
        data.loc[data.country_code == "GP", "key"] = "FR_GUA"

        # Mobility reports now have the ISO code, which makes joining with our data much easier!
        # Try to match as many records as possible using the key
        meta = aux["metadata"]
        data["_key"] = data.iso_3166_2_code.str.replace("-", "_")
        key_match_mask = data._key.isin(meta.key.values)
        data.loc[key_match_mask, "key"] = data.loc[key_match_mask, "_key"]
        data_keyed = data[~data.key.isna()].copy()

        # Drop intra-country records for which we don't have regional data
        regional_data_countries = meta.loc[~meta.subregion1_code.isna(), "country_code"].unique()
        data = data[~data.key.isna() | data.country_code.isin(regional_data_countries)]
        meta = meta[meta.country_code.isin(regional_data_countries)]

        # Try best-effort matching for records that do not have a key value
        data = data[data.key.isna()].copy()

        # Clean up known issues with subregion names
        data["match_string"] = data.apply(_process_record, axis=1)
        usa_mask = data.country_code == "US"
        data.loc[~usa_mask & ~data.subregion1_name.isna(), "subregion1_name"] = ""
        data.loc[~data.subregion2_name.isna(), "subregion2_name"] = ""

        # GB data is actually county-level even if reported as subregion1
        data.loc[data.country_code == "GB", "subregion2_name"] = ""

        return concat([data_keyed, data])
