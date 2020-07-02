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

from typing import Dict, List
from pandas import DataFrame
from lib.pipeline import DataSource
from lib.cast import safe_int_cast


class AfghanistanHumdataDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = (
            dataframes[0]
            .rename(
                columns={
                    "Date": "date",
                    "Province": "match_string",
                    "Cases": "total_confirmed",
                    "Deaths": "total_deceased",
                    "Active Cases": "current_confirmed",
                    "Recoveries": "total_recovered",
                }
            )
            .drop([0])
            .drop(axis=1, columns="current_confirmed")
        )

        # Parse integers
        for column in ("total_confirmed", "total_deceased", "total_recovered"):
            data[column] = data[column].apply(lambda x: safe_int_cast(str(x).replace(",", "")))

        # Make sure all records have the country code
        data["country_code"] = "AF"

        # Remove redundant info from names
        data.match_string = data.match_string.str.replace("Province", "")

        # Output the results
        return data
