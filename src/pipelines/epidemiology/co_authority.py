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
from lib.cast import safe_datetime_parse, age_group
from lib.pipeline import DataSource
from lib.utils import table_rename


class ColombiaDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename appropriate columns
        data = table_rename(
            dataframes[0],
            {
                "codigo divipola": "subregion2_code",
                "fecha de muerte": "date_new_deceased",
                "fecha diagnostico": "date_new_confirmed",
                "fecha recuperado": "date_new_recovered",
                "edad": "age",
                "sexo": "sex",
            },
        )

        # Clean up the subregion code
        data.subregion2_code = data.subregion2_code.apply(lambda x: "{0:05d}".format(int(x)))

        # Compute the key from the DIVIPOLA code
        data["key"] = (
            "CO_" + data.subregion2_code.apply(lambda x: x[:2]) + "_" + data.subregion2_code
        )

        # A few cases are at the l2 level
        data.key = data.key.apply(lambda x: "CO_" + x[-2:] if x.startswith("CO_00_") else x)

        # Create stratified age bands
        data.age = data.age.apply(age_group)

        # Rename the sex values
        data.sex = data.sex.apply({"M": "male", "F": "female"}.get)

        # Go from individual case records to key-grouped records in a flat table
        merged: DataFrame = None
        index_columns = ["key", "date", "sex", "age"]
        value_columns = ["new_confirmed", "new_deceased", "new_recovered"]
        for value_column in value_columns:
            subset = data.rename(columns={"date_{}".format(value_column): "date"})[index_columns]
            subset = subset[~subset.date.isna() & (subset.date != "-   -")].dropna()
            subset[value_column] = 1
            subset = subset.groupby(index_columns).sum().reset_index()
            if merged is None:
                merged = subset
            else:
                merged = merged.merge(subset, how="outer")

        # Convert date to ISO format
        merged.date = merged.date.apply(safe_datetime_parse)
        merged = merged[~merged.date.isna()]
        merged.date = merged.date.apply(lambda x: x.date().isoformat())
        merged = merged.fillna(0)

        # Group by level 2 region, and add the parts
        l2 = merged.copy()
        l2["key"] = l2.key.apply(lambda x: "_".join(x.split("_")[:2]))
        l2 = l2.groupby(index_columns).sum().reset_index()

        # Group by country level, and add the parts
        l1 = l2.copy().drop(columns=["key"])
        l1 = l1.groupby(index_columns[1:]).sum().reset_index()
        l1["key"] = "CO"

        return concat([merged, l1, l2])[index_columns + value_columns]
