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
from pandas import DataFrame, isnull
from lib.cast import safe_int_cast
from lib.pipeline import DataSource
from lib.time import datetime_isoformat, date_offset
from lib.utils import get_or_default, grouped_cumsum


class ECDCDataSource(DataSource):
    @staticmethod
    def _adjust_date(data: DataFrame, aux: DataFrame) -> DataFrame:
        """ Adjust the date of the data based on the report offset """

        # Save the current columns to filter others out at the end
        data_columns = data.columns

        # Filter auxiliary dataset to only get the relevant keys
        data = data.merge(aux, suffixes=("", "aux_"), how="left")

        # Perform date adjustment for all records so date is consistent across datasets
        data.aggregate_report_offset = data.aggregate_report_offset.apply(safe_int_cast)
        data["date"] = data.apply(
            lambda x: date_offset(x["date"], get_or_default(x, "aggregate_report_offset", 0)),
            axis=1,
        )

        return data[data_columns]

    def parse_dataframes(
        self, dataframes: List[DataFrame], metadata: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = dataframes[0]
        metadata = metadata["metadata"]

        # Ensure date field is used as a string
        data["dateRep"] = data["dateRep"].astype(str)

        # Convert date to ISO format
        data["date"] = data["dateRep"].apply(lambda x: datetime_isoformat(x, "%d/%m/%Y"))

        # Workaround for https://github.com/open-covid-19/data/issues/8
        # ECDC mistakenly labels Greece country code as EL instead of GR
        data["geoId"] = data["geoId"].apply(lambda code: "GR" if code == "EL" else code)

        # Workaround for https://github.com/open-covid-19/data/issues/13
        # ECDC mistakenly labels Great Britain country code as UK instead of GB
        data["geoId"] = data["geoId"].apply(lambda code: "GB" if code == "UK" else code)

        # Remove bogus entries (cruiseships, etc.)
        data = data[~data["geoId"].apply(lambda code: len(code) > 2)]

        data = data.rename(columns={"geoId": "key", "cases": "confirmed", "deaths": "deceased"})

        # Adjust the date of the records to match local reporting
        data = self._adjust_date(data, metadata)

        # Keep only the columns we can process
        data = data[["date", "key", "confirmed", "deceased"]]

        return grouped_cumsum(data, ["key", "date"])
