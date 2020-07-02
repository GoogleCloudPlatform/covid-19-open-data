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
from lib.cast import safe_int_cast
from lib.utils import grouped_diff, pivot_table


class VenezuelaHumDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = pivot_table(dataframes[0].set_index("date"), pivot_name="match_string").rename(
            columns={"value": "confirmed"}
        )

        # Remove cities from output
        data = data[~data.match_string.isin(["La Guaira", "Los Roques"])]

        # Compute daily differences
        data = grouped_diff(data, ["match_string", "date"])

        # Add country code and return
        data["country_code"] = "VE"
        return data
