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


class NetherlandsDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = dataframes[0].rename(
            columns={
                "Date_of_report": "date",
                "Municipality_code": "subregion2_code",
                "Municipality_name": "subregion2_name",
                "Province": "subregion1_name",
                "Total_reported": "confirmed",
                "Hospital_admission": "hospitalized",
                "Deceased": "deceased",
            }
        )

        # Drop data without a clear demarcation
        data = data[~data.subregion1_name.isna()]
        data = data[~data.subregion2_code.isna()]
        data = data[~data.subregion2_name.isna()]

        # Get date in ISO format
        data.date = data.date.apply(lambda x: datetime.fromisoformat(x).date().isoformat())

        # Make sure the region code is zero-padded and without prefix
        data["subregion2_code"] = data["subregion2_code"].apply(lambda x: x[2:])

        data = data.drop(columns=["subregion1_name", "subregion2_name"])
        data = data.merge(aux["metadata"], on="subregion2_code")

        # We only need to keep key-date pair for identification
        data = data[["date", "key", "confirmed", "deceased", "hospitalized"]]

        # Compute the daily counts
        data = grouped_diff(data, ["key", "date"])

        # Group by level 2 region, and add the parts
        l2 = data.copy()
        l2["key"] = l2.key.apply(lambda x: x[:5])
        l2 = l2.groupby(["key", "date"]).sum().reset_index()

        # Group by country level, and add the parts
        l1 = l2.copy().drop(columns=["key"])
        l1 = l1.groupby("date").sum().reset_index()
        l1["key"] = "NL"

        # Output the results
        return concat([l1, l2, data])
