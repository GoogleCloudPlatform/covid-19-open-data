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

from datetime import datetime
from typing import Any, Dict, List
from pandas import DataFrame, concat, merge
from lib.pipeline import DataSource
from lib.utils import grouped_diff


class NytCovidL2DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = dataframes[0].rename(
            columns={
                "date": "date",
                "state": "subregion1_name",
                "cases": "confirmed",
                "deaths": "deceased",
            }
        )

        # Add state code to the data
        us_meta = aux["metadata"]
        us_meta = us_meta[us_meta["country_code"] == "US"]
        us_meta = us_meta[us_meta["subregion2_code"].isna()]
        state_map = {
            idx: code
            for idx, code in us_meta.set_index("subregion1_name")["subregion1_code"].iteritems()
        }
        data["subregion1_code"] = data["subregion1_name"].apply(lambda x: state_map[x])

        # Manually build the key rather than doing automated merge for performance reasons
        data["key"] = "US_" + data["subregion1_code"]

        # Now that we have the key, we don't need any other non-value columns
        data = data[["date", "key", "confirmed", "deceased"]]
        data = grouped_diff(data, ["key", "date"])
        return data


class NytCovidL3DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = (
            dataframes[0]
            .rename(
                columns={
                    "date": "date",
                    "state": "subregion1_name",
                    "fips": "subregion2_code",
                    "cases": "confirmed",
                    "deaths": "deceased",
                }
            )
            .drop(columns=["county"])
            .dropna(subset=["subregion2_code"])
        )

        # Add state code to the data
        us_meta = aux["metadata"]
        us_meta = us_meta[us_meta["country_code"] == "US"]
        us_meta = us_meta[us_meta["subregion2_code"].isna()]
        state_map = {
            idx: code
            for idx, code in us_meta.set_index("subregion1_name")["subregion1_code"].iteritems()
        }
        data["subregion1_code"] = data["subregion1_name"].apply(lambda x: state_map[x])

        # Make sure the FIPS code is well-formatted
        data["subregion2_code"] = data["subregion2_code"].apply(lambda x: "{0:05d}".format(int(x)))

        # Manually build the key rather than doing automated merge for performance reasons
        data["key"] = "US_" + data["subregion1_code"] + "_" + data["subregion2_code"]

        # Now that we have the key, we don't need any other non-value columns
        data = data[["date", "key", "confirmed", "deceased"]]
        data = grouped_diff(data, ["key", "date"])
        return data
