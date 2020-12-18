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
from lib.pipeline import DataSource
from lib.utils import table_merge, table_rename


_col_adapter_cases = {
    "WeekStartDate": "date",
    "County": "subregion2_name",
    # "NewPos_All": "new_confirmed",
    "Age 0-19": "new_confirmed_age_00",
    "Age 20-39": "new_confirmed_age_01",
    "Age 40-59": "new_confirmed_age_02",
    "Age 60-79": "new_confirmed_age_03",
    "Age 80+": "new_confirmed_age_04",
}

_col_adapter_deaths = {
    "WeekStartDate": "date",
    "County": "subregion2_name",
    # "Deaths": "new_deceased",
    "Age 0-19": "new_deceased_age_00",
    "Age 20-39": "new_deceased_age_01",
    "Age 40-59": "new_deceased_age_02",
    "Age 60-79": "new_deceased_age_03",
    "Age 80+": "new_deceased_age_04",
}

_col_adapter_hosp = {
    "WeekStartDate": "date",
    "County": "subregion2_name",
    # "Hospitalizations": "new_hospitalized",
    "Age 0-19": "new_hospitalized_age_00",
    "Age 20-39": "new_hospitalized_age_01",
    "Age 40-59": "new_hospitalized_age_02",
    "Age 60-79": "new_hospitalized_age_03",
    "Age 80+": "new_hospitalized_age_04",
}


class WashingtonDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        rename_opts = dict(drop=True, remove_regex=r"[^0-9a-z\s]")
        data = table_merge(
            [
                table_rename(dataframes[0]["Cases"], _col_adapter_cases, **rename_opts),
                table_rename(dataframes[0]["Deaths"], _col_adapter_deaths, **rename_opts),
                table_rename(dataframes[0]["Hospitalizations"], _col_adapter_hosp, **rename_opts),
            ],
            how="outer",
            on=["date", "subregion2_name"],
        )

        data = data[data["subregion2_name"] != "Unassigned"]

        data["country_code"] = "US"
        data["subregion1_code"] = "WA"
        data["age_bin_00"] = "0-19"
        data["age_bin_01"] = "20-39"
        data["age_bin_02"] = "40-59"
        data["age_bin_03"] = "60-79"
        data["age_bin_04"] = "80-"

        return data
