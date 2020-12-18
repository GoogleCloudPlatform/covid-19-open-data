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
from lib.cast import age_group, safe_int_cast
from lib.pipeline import DataSource
from lib.time import datetime_isoformat
from lib.utils import table_rename


def _age_adapter(age: str) -> str:
    try:
        age = safe_int_cast(str(age).replace("+", "-").split("-")[0])
        return age_group(age, bin_size=10, age_cutoff=70)
    except:
        return "age_unknown"


class TaiwanDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # 確定病名,個案研判日,縣市,性別,是否為境外移入,年齡層,確定病例數
        # disease,date,county,city,sex,overseas,age,new_confirmed
        data = table_rename(
            dataframes[0],
            {
                # "確定病名": "disease",
                "個案研判日": "date",
                "縣市": "match_string",
                "性別": "sex",
                # "是否為境外移入": "overseas",
                "年齡層": "age",
                "確定病例數": "new_confirmed",
            },
            drop=True,
            remove_regex=r"[^0-9a-z\s]",
        )

        # Convert date to ISO format
        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%Y/%m/%d"))

        # Translate sex labels; only male, female and unknown are given
        sex_adapter = lambda x: {"男": "male", "女": "female"}.get(x, "sex_unknown")
        data["sex"] = data["sex"].apply(sex_adapter)

        # Normalize the age groups to be like the rest of our data
        data["age"] = data["age"].apply(_age_adapter)

        # Aggregate to country level
        country = (
            data.drop(columns=["match_string"]).groupby(["date", "age", "sex"]).sum().reset_index()
        )
        country["key"] = "TW"

        # Remove unknown data
        data = data[data["match_string"].notna()]
        data = data[data["match_string"] != "空值"]

        data["country_code"] = "TW"
        return concat([country, data])
