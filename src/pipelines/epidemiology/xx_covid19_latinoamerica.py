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

import re
from typing import Dict, List
from pandas import DataFrame
from lib.cast import safe_int_cast
from lib.pipeline import DataSource


class Covid19LatinoAmericaDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Read data from GitHub repo
        data = {}
        for df, name in zip(dataframes, ("confirmed", "deceased", "recovered")):
            df.rename(columns={"ISO 3166-2 Code": "key"}, inplace=True)
            df.key = df.key.str.replace("-", "_")
            data[name] = df.set_index("key")

        # Transform the data from non-tabulated format to record format
        records = []
        for key in data["confirmed"].index.unique():
            date_columns = [
                col for col in data["confirmed"].columns if re.match(r"\d\d\d\d-\d\d-\d\d", col)
            ]
            for col in date_columns:
                records.append(
                    {
                        **{"date": col, "key": key},
                        **{
                            f"total_{var}": safe_int_cast(df.loc[key, col])
                            for var, df in data.items()
                        },
                    }
                )

        # Zeroes can be considered NaN in this data
        data = DataFrame.from_records(records).replace(0, None)

        # Since it's cumsum data, we can forward fill relatively safely
        data = data.sort_values(["key", "date"]).groupby("key").apply(lambda x: x.ffill())

        # We already have CO data directly from the authoritative source, ideally we would match
        # the keys from here to the source which uses DIVIPOLA codes. That's left as a TODO for now.
        data = data[~data.key.str.startswith("CO_")]

        # Correct the French colonies since we are using something different to the ISO 3166-2 code
        data.loc[data.key == "FR_GP", "key"] = "FR_GUA"

        # VE_W are "external territories" of Venezuela, which can't be mapped to any particular
        # geographical region and therefore we exclude them
        data = data[data.key != "VE_W"]

        return data
