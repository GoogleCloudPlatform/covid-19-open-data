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
from pandas import DataFrame, concat, to_datetime
from lib.pipeline import DataSource
from lib.cast import age_group


class FranceDataSource(DataSource):

    column_adapter = {
        "jour": "date",
        "cl_age90": "age",
        "sexe": "sex",
        "dep": "subregion2_code",
        "reg": "subregion2_code",
        "p": "new_confirmed",
        "t": "new_tested",
        "incid_hosp": "new_hospitalized",
        "incid_dc": "new_deceased",
        "incid_rad": "new_recovered",
        "hosp": "current_hospitalized",
        "rea": "current_intensive_care",
        "rad": "total_recovered",
        "dc": "total_deceased",
    }

    region_adapter = {"971": "GUA", "972": "MQ", "973": "GF", "974": "LRE", "976": "MAY"}

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = dataframes[0].rename(columns=self.column_adapter)

        # Make sure that the department is a string
        data.subregion2_code = data.subregion2_code.apply(
            lambda x: f"{x:02d}" if isinstance(x, int) else str(x)
        )

        # Add subregion1_code field to all records
        data["subregion1_code"] = ""

        # Adjust for special regions
        for subregion2_code, subregion1_code in self.region_adapter.items():
            mask = data.subregion2_code == subregion2_code
            data.loc[mask, "subregion2_code"] = None
            data.loc[mask, "subregion1_code"] = subregion1_code

        # Get date in ISO format
        data.date = to_datetime(data.date).apply(lambda x: x.date().isoformat())

        # Get keys from metadata auxiliary table
        data["country_code"] = "FR"
        subregion1_mask = data.subregion2_code.isna()
        data1 = data[subregion1_mask].merge(
            aux["metadata"], on=("country_code", "subregion1_code", "subregion2_code")
        )
        data2 = (
            data[~subregion1_mask]
            .drop(columns=["subregion1_code"])
            .merge(aux["metadata"], on=("country_code", "subregion2_code"))
        )
        data = concat([data1, data2])

        # We only need to keep key-date pair for identification
        data = data.drop(columns=["subregion1_code", "subregion2_code"])

        # Use age as an indexing key only if it exists
        extra_indexing_columns = []
        if "age" in data.columns:
            # Zero means "no age group" which we don't care about
            data = data[data.age > 0]
            # Convert to the expected age range format to stratify later
            data.age = data.age.apply(age_group)
            # Add 'age' to the indexing columns
            extra_indexing_columns += ["age"]

        # Use sex as an index key only if it exists
        if "sex" in data.columns:
            # Zero means ungrouped, which we don't care about
            data = data[data.sex > 0]
            # Convert to known variable names
            data.sex = data.sex.apply({1: "male", 2: "female"}.get)
            # Add 'dex' to the indexing columns
            extra_indexing_columns += ["sex"]

        # Group by level 2 region, and add the parts
        l2 = data.copy()
        l2["key"] = l2.key.apply(lambda x: "_".join(x.split("_")[:2]))
        l2 = l2.groupby(extra_indexing_columns + ["key", "date"]).sum().reset_index()

        # Group by country level, and add the parts
        l1 = l2.copy().drop(columns=["key"])
        l1 = l1.groupby(extra_indexing_columns + ["date"]).sum().reset_index()
        l1["key"] = "FR"

        # Country-level data greatly differs from what's available from other sources, because only
        # hospitalization records are available via France's authoritative source. For that reason,
        # we remove the data types which can be obtained by other means at the country level.
        drop_country_columns = ["new_confirmed", "new_tested"]
        if any([col in l1.columns for col in drop_country_columns]):
            l1 = DataFrame(data=[], columns=l1.columns)

        # Remove unnecessary columns (needed for concat to work properly)
        data = data.drop(
            columns=[col for col in aux["metadata"].columns if col in data and col != "key"]
        )

        # Output the results
        return concat([l1, l2, data])
