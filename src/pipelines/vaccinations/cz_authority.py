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
from pandas import DataFrame, concat, isna
from lib.case_line import convert_cases_to_time_series

from lib.data_source import DataSource
from lib.time import datetime_isoformat
from lib.utils import aggregate_admin_level, table_rename


_column_adapter = {
    "datum": "date",
    "vekova_skupina": "age",
    # "vakcina": "",
    "kraj_nuts_kod": "subregion1_code",
    "prvnich_davek": "new_persons_vaccinated",
    "druhych_davek": "new_persons_fully_vaccinated",
    "celkem_davek": "new_vaccine_doses_administered",
}


class CzechRepublicDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename appropriate columns
        data = table_rename(dataframes[0], _column_adapter)
        data["key"] = "CZ_" + data.subregion1_code.str.replace("CZ0", "")

        # Convert all dates to ISO format
        data["date"] = (
            data["date"]
            .astype(str)
            .apply(lambda x: datetime_isoformat(x, "%d.%m.%Y" if "." in x else "%Y-%m-%d"))
        )

        # Aggregate here since some of the codes are null (04 indicates either BZ/TN)
        country = aggregate_admin_level(data, ["date", "age"], "country")
        country["key"] = "CZ"

        # Remove bogus values
        data = data[data["key"] != "CZ_99"]
        data = data[data["key"] != "CZ_99_99Y"]

        return concat([country, data])
