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
from lib.cast import safe_int_cast
from lib.io import read_file
from lib.pipeline import DataSource
from lib.time import datetime_isoformat
from lib.utils import table_rename


class LuxembourgDataSource(DataSource):
    def parse(self, sources: List[str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        data = read_file(sources[0], error_bad_lines=False, encoding="ISO-8859-1")
        data = table_rename(
            data,
            {
                "Date": "date",
                "Nombre de personnes en soins normaux": "current_hospitalized",
                "Nombre de personnes en soins intensifs (sans patients du Grand Est)": "current_intensive_care",
                "Nombre de décès - cumulé (sans patients du Grand Est)": "total_deceased",
                "Total patients COVID ayant quitté l'hôpital (hospitalisations stationnaires, données brutes)": "new_recovered",
                "Nombre de nouvelles personnes testées COVID+ par jour ": "new_tested",
            },
        )

        # Get date in ISO format
        data.date = data.date.apply(lambda x: datetime_isoformat(x, "%d/%m/%Y"))

        # Keep only columns we can provess
        data = data[
            [
                "date",
                "current_hospitalized",
                "current_intensive_care",
                "total_deceased",
                "new_recovered",
                "new_tested",
            ]
        ]

        # Convert recovered into a number
        data.new_recovered = data.new_recovered.apply(lambda x: safe_int_cast(x.replace("-", "0")))

        # Only country-level data is provided
        data["key"] = "LU"

        # Output the results
        return data
