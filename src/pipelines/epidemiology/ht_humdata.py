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
from lib.data_source import DataSource
from lib.time import datetime_isoformat
from lib.utils import table_rename


class HaitiHumdataDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        # Data has two sheets, one as of May 5, and one for all subsequent dates.
        # They have different column-names, so we normalize and combine.
        dataframe = dataframes[0]

        # Confirmed cases in asofMay5 is the same as Cumulative cases in fromMay6
        # Deaths in asofMay5 is the same as Cumulative deaths in fromMay6
        dataframe["data_asofMay5"] = table_rename(
            dataframe["data_asofMay5"],
            {
                "Date": "date",
                "Confirmed cases": "total_confirmed",
                "Deaths": "total_deceased",
                "Département": "match_string",
            },
            drop=True,
        )

        dataframe["data_fromMay6"] = table_rename(
            dataframe["data_fromMay6"],
            {
                "Date": "date",
                "Cumulative cases": "total_confirmed",
                "Cumulative Deaths": "total_deceased",
                "Département": "match_string",
            },
            drop=True,
        )

        # Concatenate the two Series and drop the first row which is a column description.
        data = concat([dataframe["data_asofMay5"], dataframe["data_fromMay6"]]).drop(0)

        data.date = data.date.apply(lambda x: datetime_isoformat(x, "%Y-%m-%d %H:%M:%S"))

        # Make sure all records have the country code
        data["country_code"] = "HT"

        # Output the results
        return data
