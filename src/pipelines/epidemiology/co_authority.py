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
from datetime import datetime
from lib.case_line import convert_cases_to_time_series
from lib.cast import safe_datetime_parse
from lib.data_source import DataSource
from lib.utils import table_rename


class ColombiaDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
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
                "Pertenencia etnica": "ethnicity",
            },
        )

        # Clean up the subregion code
        data.subregion2_code = data.subregion2_code.apply(lambda x: "{0:05d}".format(int(x)))

        # Compute the key from the DIVIPOLA code
        data["key"] = (
            "CO_" + data.subregion2_code.apply(lambda x: x[:2]) + "_" + data.subregion2_code
        )

        # A few cases are at the l2 level
        data["key"] = data["key"].apply(lambda x: "CO_" + x[-2:] if x.startswith("CO_00_") else x)

        # Go from individual case records to key-grouped records in a flat table
        index_columns = ["key", "date", "sex", "age"]
        value_columns = ["new_confirmed", "new_deceased", "new_recovered"]
        merged = convert_cases_to_time_series(data)

        # Some dates are badly formatted as 31/12/1899 in the raw data we can drop these.
        merged = merged[(merged["date"] != datetime(1899, 12, 31))].dropna(subset=["date"])

        # Parse dates to ISO format.
        merged["date"] = merged["date"].apply(safe_datetime_parse)
        merged["date"] = merged["date"].apply(lambda x: x.date().isoformat())

        # Group by level 2 region, and add the parts
        l2 = merged.copy()
        l2["key"] = l2.key.apply(lambda x: "_".join(x.split("_")[:2]))
        l2 = l2.groupby(index_columns).sum().reset_index()

        # Group by country level, and add the parts
        l1 = l2.copy().drop(columns=["key"])
        l1 = l1.groupby(index_columns[1:]).sum().reset_index()
        l1["key"] = "CO"

        return concat([merged, l1, l2])[index_columns + value_columns]
