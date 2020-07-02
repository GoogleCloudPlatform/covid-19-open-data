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
from pandas import DataFrame, concat, merge, to_datetime
from lib.cast import age_group, safe_datetime_parse
from lib.io import read_file
from lib.pipeline import DataSource
from lib.utils import table_rename


class PhilippinesDataSource(DataSource):
    def parse(self, sources: List[str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        data = read_file(sources[0], sheet_name="Sheet3")

        # Rename appropriate columns
        data = table_rename(
            data,
            {
                "ProvRes": "match_string_province",
                "RegionRes": "match_string_region",
                "DateDied": "date_new_deceased",
                "DateSpecimen": "date_new_confirmed",
                "DateRecover": "date_new_recovered",
                "Age": "age",
                "Sex": "sex",
            },
        )

        # When there is recovered removal, but missing recovery date, estimate it
        missing_recovery_mask = data.date_new_recovered.isna() & (data.removaltype == "Recovered")
        data.loc[missing_recovery_mask, "date_new_recovered"] = data.loc[
            missing_recovery_mask, "datereprem"
        ]

        # When there is deceased removal, but missing recovery date, estimate it
        missing_deceased_mask = data.date_new_deceased.isna() & (data.removaltype == "Died")
        data.loc[missing_deceased_mask, "date_new_deceased"] = data.loc[
            missing_deceased_mask, "datereprem"
        ]

        # Hospitalized is estimated as the same date as confirmed if admitted == yes
        data["date_new_hospitalized"] = None
        admitted_mask = data.admitted.str.lower() == "yes"
        data.loc[admitted_mask, "date_new_hospitalized"] = data.loc[
            admitted_mask, "date_new_confirmed"
        ]

        # Create stratified age bands
        data.age = data.age.apply(age_group)

        # Rename the sex values
        data.sex = data.sex.apply(lambda x: x.lower())

        # Go from individual case records to key-grouped records in a flat table
        merged: DataFrame = None
        index_columns = ["match_string_province", "match_string_region", "date", "sex", "age"]
        value_columns = ["new_confirmed", "new_deceased", "new_recovered", "new_hospitalized"]
        for value_column in value_columns:
            subset = data.rename(columns={"date_{}".format(value_column): "date"})[index_columns]
            subset = subset[~subset.date.isna() & (subset.date != "-   -")].dropna()
            subset[value_column] = 1
            subset = subset.groupby(index_columns).sum().reset_index()
            subset.date = to_datetime(subset.date)
            if merged is None:
                merged = subset
            else:
                merged = merged.merge(subset, how="outer")

        # Convert date to ISO format
        merged.date = merged.date.apply(safe_datetime_parse)
        merged = merged[~merged.date.isna()]
        merged.date = merged.date.apply(lambda x: x.date().isoformat())
        merged = merged.fillna(0)

        # Aggregate regions and provinces separately
        l3 = merged.rename(columns={"match_string_province": "match_string"})
        l2 = merged.rename(columns={"match_string_region": "match_string"})
        l2.match_string = l2.match_string.apply(lambda x: x.split(": ")[-1])

        # Ensure matching by flagging whether a record must be L2 or L3
        l2["subregion2_code"] = None
        l3["subregion2_code"] = ""

        data = concat([l2, l3]).dropna(subset=["match_string"])
        data = data[data.match_string != "Repatriate"]
        data["country_code"] = "PH"

        return data
