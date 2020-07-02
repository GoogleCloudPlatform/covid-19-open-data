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

from typing import Any, Dict, List
from pandas import DataFrame, concat, merge
from lib.pipeline import DataSource
from lib.time import datetime_isoformat
from lib.utils import grouped_diff


class CanadaDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = (
            dataframes[0]
            .rename(
                columns={
                    "prname": "subregion1_name",
                    "numconf": "confirmed",
                    "numdeaths": "deceased",
                    "numtested": "tested",
                    "numrecover": "recovered",
                }
            )
            .drop(columns=["prnameFR"])
        )

        # Convert date to ISO format
        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%d-%m-%Y"))

        # Compute the daily counts
        data = grouped_diff(data, ["subregion1_name", "date"])

        # Make sure all records have the country code
        data["country_code"] = "CA"

        # Country-level records should have null region name
        country_mask = data["subregion1_name"] == "Canada"
        data.loc[country_mask, "subregion1_name"] = None

        # Remove bogus data
        data = data[~data["subregion1_name"].apply(lambda x: "traveller" in (x or "").lower())]

        # Output the results
        return data
