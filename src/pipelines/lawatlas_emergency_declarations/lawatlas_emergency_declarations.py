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
from lib.data_source import DataSource
from lib.cast import safe_int_cast
from lib.time import date_range


def _convert_column_name(col: str) -> str:
    """
    We rename some columns from the original dataset to make things a little more clear and to add
    the prefix `lawatlas_` to all the dataset-specific columns.
    """
    col = col.lower()
    col = col.replace("'", "")
    col = col.replace(" ", "_")
    col = col.replace("_appl", "_statewide")
    col = col.replace("_type2", "_type")
    col = col.replace("med_restr", "med_restrict")
    col = col.replace("_requirement", "_req")
    col = col.replace("_req", "_requirement")
    return "lawatlas_" + col


class LawAtlasDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Parse the data as a list of records
        records = []
        for _, row in dataframes[0].iterrows():
            row = row.to_dict()

            # Parse the start and end dates
            date_start = row["Effective Date"][:10]
            date_end = row["Valid Through Date"][:10]

            # Convert column name and delete unnecessary columns
            row["subregion1_name"] = row["Jurisdictions"]
            del row["Jurisdictions"]
            del row["Effective Date"]
            del row["Valid Through Date"]

            # Insert a record for each date in the range
            for date in date_range(date_start, date_end):
                record = {}
                record["date"] = date
                non_numeric_columns = ("date", "subregion1_name")
                for col, val in row.items():
                    if col in non_numeric_columns:
                        record[col] = val
                    else:
                        record[_convert_column_name(col)] = safe_int_cast(val)
                records.append(record)

        # Convert to DataFrame and add metadata for matching
        data = DataFrame.from_records(records)
        data["country_code"] = "US"
        data["subregion2_code"] = None
        data["locality_code"] = None

        return data
