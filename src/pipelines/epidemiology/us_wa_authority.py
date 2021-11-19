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
from lib.pipeline import DataSource
from lib.time import datetime_isoformat
from lib.utils import table_merge, table_rename

_col_adapter_base = {
    "WeekStartDate": "date",
    "County": "subregion2_name",
    "Age 0-11": "new_stat_age_00",
    "Age 12-19": "new_stat_age_01",
    "Age 20-34": "new_stat_age_02",
    "Age 35-49": "new_stat_age_03",
    "Age 50-64": "new_stat_age_04",
    "Age 65-79": "new_stat_age_05",
    "Age 80+": "new_stat_age_06",
}


class WashingtonDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        tables = []
        rename_opts = dict(drop=True, remove_regex=r"[^0-9a-z\s]")
        name_map = {"Cases": "confirmed", "Deaths": "deceased", "Hospitalizations": "hospitalized"}
        for sheet_name, stat_name in name_map.items():
            col_name = f"_{stat_name}_"
            col_adapter = {k: v.replace("_stat_", col_name) for k, v in _col_adapter_base.items()}
            table = table_rename(dataframes[0][sheet_name], col_adapter, **rename_opts)
            table["date"] = table["date"].apply(lambda x: str(x)[:10])
            tables.append(table)

        data = table_merge(tables, how="outer", on=["date", "subregion2_name"])
        state = data.drop(columns=["subregion2_name"]).groupby(["date"]).sum().reset_index()
        state["key"] = "US_WA"

        data = data[data["subregion2_name"] != "Unassigned"]
        data["country_code"] = "US"
        data["subregion1_code"] = "WA"

        for df in (state, data):
            df["age_bin_00"] = "0-11"
            df["age_bin_01"] = "12-19"
            df["age_bin_02"] = "20-34"
            df["age_bin_03"] = "35-49"
            df["age_bin_04"] = "50-64"
            df["age_bin_05"] = "65-79"
            df["age_bin_06"] = "80-"

        return concat([state, data])
