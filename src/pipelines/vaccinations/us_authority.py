# Copyright 2021 Google LLC
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

import json
from typing import Dict
from pandas import DataFrame
from lib.data_source import DataSource
from lib.utils import table_rename


class CDCDataSource(DataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        dataframes = {}

        trends_field_name = "vaccination_trends_data"
        with open(sources[trends_field_name], "r") as fh:
            dataframes[trends_field_name] = DataFrame.from_records(json.load(fh)[trends_field_name])

        return self.parse_dataframes(dataframes, aux, **parse_opts)

    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(
            dataframes["vaccination_trends_data"],
            {
                "Location": "key",
                "Date": "date",
                "Date_Type": "_date_type",
                "Administered_Daily": "new_vaccine_doses_administered",
                "Administered_Cumulative": "total_vaccine_doses_administered",
                "Admin_Dose_1_Daily": "new_persons_vaccinated",
                "Admin_Dose_1_Cumulative": "total_persons_vaccinated",
                "Admin_Dose_2_Daily": "new_persons_fully_vaccinated",
                "Admin_Dose_2_Cumulative": "total_persons_fully_vaccinated",
            },
            drop=True,
            remove_regex=r"[^0-9a-z\s]",
        )

        data = data[data["key"] == "US"]
        data = data[data["_date_type"] == "Admin"]
        data = data.sort_values("date")
        data = data.drop(columns=["_date_type"])
        return data
