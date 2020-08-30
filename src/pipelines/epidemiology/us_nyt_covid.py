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

from typing import Dict
from pandas import DataFrame
from lib.data_source import DataSource


def _add_state_code(data: DataFrame, metadata: DataFrame) -> DataFrame:

    # Add subregion1 code to the data
    us_meta = metadata
    us_meta = us_meta[us_meta["country_code"] == "US"]
    us_meta = us_meta.set_index("subregion1_name")["subregion1_code"].drop_duplicates()
    country_map = {idx: code for idx, code in us_meta.iteritems()}
    data["subregion1_code"] = data["subregion1_name"].apply(country_map.get)

    # All subregion codes should be found but sometimes we only have a subset available when
    # the pipeline is run in a test environment
    data = data.dropna(subset=["subregion1_code"])

    return data


class NytCovidL2DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = dataframes[0].rename(
            columns={
                "date": "date",
                "state": "subregion1_name",
                "cases": "total_confirmed",
                "deaths": "total_deceased",
            }
        )

        # Add state code to the data
        data = _add_state_code(data, aux["metadata"])

        # Manually build the key rather than doing automated merge for performance reasons
        data["key"] = "US_" + data["subregion1_code"]
        return data


class NytCovidL3DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = (
            dataframes[0]
            .rename(
                columns={
                    "date": "date",
                    "state": "subregion1_name",
                    "fips": "subregion2_code",
                    "cases": "total_confirmed",
                    "deaths": "total_deceased",
                }
            )
            .dropna(subset=["subregion2_code"])
        )

        # Add state code to the data
        data = _add_state_code(data, aux["metadata"])

        # Make sure the FIPS code is well-formatted
        data["subregion2_code"] = data["subregion2_code"].apply(lambda x: "{0:05d}".format(int(x)))

        # Manually build the key rather than doing automated merge for performance reasons
        data["key"] = "US_" + data["subregion1_code"] + "_" + data["subregion2_code"]

        return data
