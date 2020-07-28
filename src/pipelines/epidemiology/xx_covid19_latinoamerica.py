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
from typing import Dict
from pandas import DataFrame, isna
from lib.cast import safe_int_cast
from lib.data_source import DataSource


class Covid19LatinoAmericaDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Read data from GitHub repo
        data = {}
        for name, df in dataframes.items():
            df.rename(columns={"ISO 3166-2 Code": "key"}, inplace=True)
            df.key = df.key.str.replace("-", "_")
            data[name] = df.set_index("key")
        value_columns = [f"total_{statistic}" for statistic in data.keys()]

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
                            f"total_{statistic}": safe_int_cast(df.loc[key, col])
                            for statistic, df in data.items()
                        },
                    }
                )

        # Zeroes can be considered NaN in this data
        data = DataFrame.from_records(records).replace(0, None)

        # Remove all data without a proper date
        data = data.dropna(subset=["date"])

        # Since it's cumsum data, we can forward fill relatively safely
        data = data.sort_values(["key", "date"]).groupby("key").apply(lambda x: x.ffill())

        # We already have AR, BR, CO, MX and PE data directly from the authoritative source
        for country_code in ("AR", "BR", "CL", "CO", "MX", "PE"):
            data = data[~data.key.str.startswith(f"{country_code}_")]

        # Correct the French colonies since we are using something different to the ISO 3166-2 code
        data.loc[data.key == "FR_GP", "key"] = "FR_GUA"

        # VE_W are "external territories" of Venezuela, which can't be mapped to any particular
        # geographical region and therefore we exclude them
        data = data[data.key != "VE_W"]

        # The data appears to be very unreliable prior to March 12
        data.loc[data["date"] <= "2020-03-12", value_columns] = None

        # In many cases, total counts go down dramatically; filter out all dates prior to that
        # We lose a lot of data by setting such a low threshold, but this data source is unreliable
        skip_threshold = 0
        for key in data["key"].unique():
            rm_date = "2020-01-01"
            subset = data[data["key"] == key]
            diffs = subset.set_index("date")[value_columns].diff()
            for col in value_columns:
                max_date = diffs.loc[diffs[col] < skip_threshold].index.max()
                if not isna(max_date) and max_date > rm_date:
                    rm_date = max_date
            data.loc[(data["key"] == key) & (data["date"] < rm_date), value_columns] = None

        return data
