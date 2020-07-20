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
from pandas import DataFrame, concat
from lib.pipeline import DataSource
from lib.utils import table_rename
from ..epidemiology.cl_authority import _extract_cities


class ChileRegionsDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = table_rename(
            dataframes["intensive_care"],
            {"fecha": "date", "numero": "current_intensive_care", "Region": "match_string"},
            drop=True,
        )

        # Convert date to ISO format
        data["date"] = data["date"].astype(str)

        # Extract cities from the regions
        city = _extract_cities(data)

        # Make sure all records have country code and no subregion code
        data["country_code"] = "CL"
        data["subregion2_code"] = None

        # Drop bogus records from the data
        data.dropna(subset=["date", "match_string"], inplace=True)

        return concat([data, city])
