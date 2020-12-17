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
from lib.cast import age_group
from lib.pipeline import DataSource
from lib.utils import table_rename


_SUBREGION1_CODE_MAP = {
    "Harju": "37",
    "Hiiu": "39",
    "Ida-Viru": "45",
    "Jõgeva": "50",
    "Järva": "52",
    "Lääne": "56",
    "Lääne-Viru": "60",
    "Põlva": "64",
    "Pärnu": "68",
    "Rapla": "71",
    "Saare": "74",
    "Tartu": "79",
    "Valga": "81",
    "Viljandi": "84",
    "Võru": "87",
}


def _parse_age_bin(age_bin: str) -> str:
    try:
        return age_group(int(age_bin.split("-", 1)[0]))
    except:
        return "age_unknown"


class EstoniaDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = table_rename(
            dataframes[0],
            {
                # "id": "",
                "Gender": "sex",
                "AgeGroup": "age",
                # "Country": "country_name",
                "County": "subregion1_name",
                "ResultValue": "_test_result",
                "StatisticsDate": "date",
            },
            drop=True,
            remove_regex=r"[^0-9a-z\s]",
        )

        data["new_tested"] = 1
        data["new_confirmed"] = 0
        data.loc[data["_test_result"] == "P", "new_confirmed"] = 1
        data.drop(columns=["_test_result"], inplace=True)

        # Translate sex labels; only male, female and unknown are given
        sex_adapter = lambda x: {"M": "male", "N": "female"}.get(x, "sex_unknown")
        data["sex"] = data["sex"].apply(sex_adapter)

        # Normalize age group labels
        data["age"] = data["age"].apply(_parse_age_bin)

        # Use proper ISO codes for the subregion1 level
        data["subregion1_name"] = data["subregion1_name"].str.replace(" maakond", "")
        data["subregion1_code"] = data["subregion1_name"].apply(_SUBREGION1_CODE_MAP.get)
        data.drop(columns=["subregion1_name"], inplace=True)

        # Aggregate country-level data by adding all counties
        country = (
            data.drop(columns=["subregion1_code"])
            .groupby(["date", "age", "sex"])
            .sum()
            .reset_index()
        )
        country["key"] = "EE"

        # We can build the key for the data directly from the subregion codes
        data["key"] = "EE_" + data["subregion1_code"]

        # Drop bogus records from the data
        data.dropna(subset=["subregion1_code"], inplace=True)

        return concat([country, data])
