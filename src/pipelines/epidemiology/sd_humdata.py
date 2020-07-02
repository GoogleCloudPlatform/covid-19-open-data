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

from typing import Any, Dict, List
from pandas import DataFrame, concat, merge
from lib.pipeline import DataSource
from lib.cast import safe_int_cast
from lib.time import datetime_isoformat
from lib.utils import grouped_cumsum


class SudanHumdataDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = (
            dataframes[0]
            .rename(
                columns={
                    "Report Date": "date",
                    "State": "match_string",
                    "Confirmed Cases": "total_confirmed",
                }
            )
            .drop([0])
        )

        # The dates in the provided CSV are incorrect for one of the reports.
        # Replace with report date taken from text of report.
        data.loc[
            data["Source"]
            == "https://reliefweb.int/sites/reliefweb.int/files/resources/Situation%20Report%20-%20Sudan%20-%207%20May%202020.pdf",
            "date",
        ] = "5/11/2020"

        data = data.drop(axis=1, columns=["As of Date", "Source"])

        # Remove Abyei PCA, a disputed region with no data shown.
        data = data[data["match_string"] != "Abyei PCA"]

        # Data source uses different spelling from src/data/iso_3166_2_codes.csv
        data["match_string"].replace({"Gedaref": "Al Qadarif"}, inplace=True)

        data.date = data.date.apply(lambda x: datetime_isoformat(x, "%m/%d/%Y"))

        # Sudan data includes empty cells where there are no confirmed cases.
        # These get read in as NaN.  Replace them with zeroes so that the
        # grouped_diff call to get new confirmed cases works for a state's first
        # day with a case.

        data["total_confirmed"] = (
            data["total_confirmed"].fillna(0).astype({"total_confirmed": "int64"})
        )

        # Make sure all records have the country code
        data["country_code"] = "SD"

        # Output the results
        return data
