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
from lib.io import read_file
from lib.data_source import DataSource
from lib.time import datetime_isoformat


class ISCIIIHospitalizedDataSource(DataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        # Retrieve the CSV files from https://covid19.isciii.es
        data = (
            read_file(sources[0], error_bad_lines=False, encoding="ISO-8859-1")
            .rename(
                columns={
                    "FECHA": "date",
                    "CCAA": "subregion1_code",
                    "Fallecidos": "total_deceased",
                    "Hospitalizados": "total_hospitalized",
                    "UCI": "total_intensive_care",
                }
            )
            .dropna(subset=["date"])
        )

        # Confirmed cases are split across 2 columns
        confirmed_columns = ["CASOS", "PCR+"]
        for col in confirmed_columns:
            data[col] = data[col].fillna(0)
        data["total_confirmed"] = data.apply(
            lambda x: sum([x[col] for col in confirmed_columns]), axis=1
        )

        # Convert dates to ISO format
        data["date"] = data["date"].apply(lambda date: datetime_isoformat(date, "%d/%m/%Y"))

        # Keep only the columns we can process
        data = data[
            [
                "date",
                "subregion1_code",
                "total_confirmed",
                "total_deceased",
                "total_hospitalized",
                "total_intensive_care",
            ]
        ]

        # Derive the key from the subregion code
        data["key"] = "ES_" + data["subregion1_code"]

        # Output the results
        return data
