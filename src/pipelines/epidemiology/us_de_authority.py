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
from lib.time import datetime_isoformat
from lib.utils import table_rename


class DelawareDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = dataframes[0]
        data["date"] = (
            data.Year.astype(str) + "-" + data.Month.astype(str) + "-" + data.Day.astype(str)
        )

        # Ensure all dates have the appropriate format, drop the rest
        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%Y-%m-%d"))
        data = data.dropna(subset=["date"])

        # Ignore all columns which have fancy units
        data = data[data.Unit == "people"]

        # Pivot the data from indicator format to tidy format
        data = data.pivot_table(index="date", columns=["Statistic"], values="Value").reset_index()

        # Add key and age bin information to the data
        data["key"] = "US_DE"
        data["age_bin_00"] = "0-4"
        data["age_bin_01"] = "5-17"
        data["age_bin_02"] = "18-34"
        data["age_bin_03"] = "35-49"
        data["age_bin_04"] = "50-64"
        data["age_bin_05"] = "65-"

        return table_rename(
            data,
            {
                "date": "date",
                "Confirmed Deaths": "total_deceased",
                "Cumulative Number of Positive Cases": "total_confirmed",
                "Cumulative Number of Positive Cases (Age: 0-4)": "total_confirmed_age_00",
                "Cumulative Number of Positive Cases (Age: 5-17)": "total_confirmed_age_01",
                "Cumulative Number of Positive Cases (Age: 18-34)": "total_confirmed_age_02",
                "Cumulative Number of Positive Cases (Age: 35-49)": "total_confirmed_age_03",
                "Cumulative Number of Positive Cases (Age: 50-64)": "total_confirmed_age_04",
                "Cumulative Number of Positive Cases (Age: 65+)": "total_confirmed_age_05",
                "Cumulative Number of Positive Cases (Race/Ethnicity: Asian/Pacific Islander)": "total_confirmed_asian",
                "Cumulative Number of Positive Cases (Race/Ethnicity: Hispanic/Latino)": "total_confirmed_hispanic",
                "Cumulative Number of Positive Cases (Race/Ethnicity: Non-Hispanic Black)": "total_confirmed_black",
                "Cumulative Number of Positive Cases (Race/Ethnicity: Non-Hispanic White)": "total_confirmed_white",
                "Cumulative Number of Positive Cases (Sex: Male)": "total_confirmed_male",
                "Cumulative Number of Positive Cases (Sex: Female)": "total_confirmed_female",
                "Deaths (Age: 0-4)": "total_deceased_age_00",
                "Deaths (Age: 5-17)": "total_deceased_age_01",
                "Deaths (Age: 18-34)": "total_deceased_age_02",
                "Deaths (Age: 35-49)": "total_deceased_age_03",
                "Deaths (Age: 50-64)": "total_deceased_age_04",
                "Deaths (Age: 65+)": "total_deceased_age_05",
                "Deaths (Race/Ethnicity: Asian/Pacific Islander)": "total_deceased_asian",
                "Deaths (Race/Ethnicity: Hispanic/Latino)": "total_deceased_hispanic",
                "Deaths (Race/Ethnicity: Non-Hispanic Black)": "total_deceased_black",
                "Deaths (Race/Ethnicity: Non-Hispanic White)": "total_deceased_white",
                "Deaths (Sex: Female)": "total_deceased_female",
                "Deaths (Sex: Male)": "total_deceased_male",
                "Total Persons Tested (Age: 0-4)": "total_tested_age_00",
                "Total Persons Tested (Age: 5-17)": "total_tested_age_01",
                "Total Persons Tested (Age: 18-34)": "total_tested_age_02",
                "Total Persons Tested (Age: 35-49)": "total_tested_age_03",
                "Total Persons Tested (Age: 50-64)": "total_tested_age_04",
                "Total Persons Tested (Age: 65+)": "total_tested_age_05",
                "New Positive Cases": "new_confirmed",
                "Recovered": "total_recovered",
                "Total Persons Tested": "total_tested",
                "Total Persons Tested (Race/Ethnicity: Asian/Pacific Islander)": "total_tested_asian",
                "Total Persons Tested (Race/Ethnicity: Hispanic/Latino)": "total_tested_hispanic",
                "Total Persons Tested (Race/Ethnicity: Non-Hispanic Black)": "total_tested_black",
                "Total Persons Tested (Race/Ethnicity: Non-Hispanic White)": "total_tested_white",
                "Total Persons Tested (Sex: Female)": "total_tested_female",
                "Total Persons Tested (Sex: Male)": "total_tested_male",
            },
            remove_regex=r"[^\w\s\-]",
        )
