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
from lib.cast import safe_int_cast
from lib.data_source import DataSource
from lib.utils import table_rename


class ImperialDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = table_rename(
            dataframes[0],
            {
                "date": "date",
                "age": "age",
                "cum.deaths": "total_deceased",
                "daily.deaths": "new_deceased",
                "code": "subregion1_code",
            },
            drop=True,
        )

        # Convert date to ISO format
        data["date"] = data["date"].apply(lambda x: str(x)[:10])

        # Correct an error in the age bins from data source
        data.loc[data["age"] == "-1-9", "age"] = "1-9"

        # Parse age to match our group names
        def parse_age_group(age_group: str):
            new_age_group = ""
            age_bins = age_group.strip().replace("+", "-").split("-", 1)
            age_lo = safe_int_cast(age_bins[0])
            new_age_group += f"{age_lo:02d}-"

            if len(age_bins) > 1 and age_bins[1]:
                age_hi = safe_int_cast(age_bins[1])
                new_age_group += f"{age_hi:02d}"

            return new_age_group

        data["age"] = data["age"].apply(parse_age_group)

        # Derive key from the subregion code
        data["key"] = "US_" + data["subregion1_code"]

        # Some of the places are not US states
        data.loc[data["subregion1_code"] == "NYC", "key"] = "US_NY_NYC"

        # Compute our own age groups since they are not uniform across states
        for idx in range(10):
            data[f"age_bin_{idx:02d}"] = None
            data[f"new_deceased_age_{idx:02d}"] = None
            data[f"total_deceased_age_{idx:02d}"] = None
        for key in data["key"].unique():
            mask = data["key"] == key
            age_bins = data.loc[mask, "age"].unique()
            sorted_age_bins = sorted(age_bins, key=lambda x: safe_int_cast(x.split("-")[0]))
            for idx, age_bin_val in enumerate(sorted_age_bins):
                data.loc[mask, f"age_bin_{idx:02d}"] = age_bin_val
                age_bin_mask = mask & (data["age"] == age_bin_val)
                data.loc[age_bin_mask, f"new_deceased_age_{idx:02d}"] = data.loc[
                    age_bin_mask, "new_deceased"
                ]
                data.loc[age_bin_mask, f"total_deceased_age_{idx:02d}"] = data.loc[
                    age_bin_mask, "total_deceased"
                ]

        # Output the results
        return data.drop(columns=["age", "subregion1_code"])
