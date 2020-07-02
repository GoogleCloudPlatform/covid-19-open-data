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
from pandas import DataFrame, concat
from lib.pipeline import DataSource
from lib.cast import safe_int_cast
from lib.utils import table_rename


class IraqHumdataDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = table_rename(
            dataframes[0],
            {
                "date": "date",
                "Governorate": "match_string",
                "Cases": "total_confirmed",
                "Deaths": "total_deceased",
                "Recoveries": "total_recovered",
                "Active Cases": "current_confirmed",
            },
        ).iloc[1:]

        # Convert dates to ISO format
        data["date"] = data["date"].astype(str)

        # Convert value columns to appropriate types
        value_columns = [
            "total_confirmed",
            "total_deceased",
            "total_recovered",
            "current_confirmed",
        ]
        for column in value_columns:
            data[column] = data[column].apply(safe_int_cast)

        # Some regions appear split in the data
        baghdad_mask = data.match_string.apply(lambda x: "baghdad" in str(x).lower())
        baghdad_subset = data.loc[baghdad_mask]
        baghdad_subset["match_string"] = "baghdad"
        baghdad_subset = baghdad_subset.groupby(["date", "match_string"]).sum().reset_index()
        data = concat([data.loc[~baghdad_mask], baghdad_subset])

        data["country_code"] = "IQ"
        return data
