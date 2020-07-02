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

from typing import Dict, List
from pandas import DataFrame, concat, isna
from lib.cast import age_group
from lib.pipeline import DataSource
from lib.utils import table_rename


_column_adapter = {
    "datum": "date",
    "vek": "age",
    "pohlavi": "sex",
    "kraj_nuts_kod": "subregion1_code",
    "okres_lau_kod": "subregion2_code",
    "kumulativni_pocet_nakazenych": "total_confirmed",
    "kumulativni_pocet_vylecenych": "total_recovered",
    "kumulativni_pocet_umrti": "total_deceased",
    "prirustkovy_pocet_testu": "new_tested",
    "kumulativni_pocet_testu": "total_tested",
}


def _aggregate_regions(data: DataFrame, index_columns: List[str]) -> DataFrame:
    l2 = data.drop(columns=["subregion2_code"])
    l2 = l2.groupby(index_columns).sum().reset_index()

    l2["key"] = "CZ_" + l2.subregion1_code
    data["key"] = "CZ_" + data.subregion1_code + "_" + data.subregion2_code

    l2 = l2.drop(columns=["subregion1_code"])
    data = data.drop(columns=["subregion1_code", "subregion2_code"])

    return concat([l2, data])


def _parse_region_codes(data: DataFrame) -> DataFrame:
    data = data.copy()
    data["subregion2_code"] = data.subregion2_code.str.replace("CZ0", "")
    data["subregion1_code"] = data.subregion1_code.str.replace("CZ0", "")
    data["subregion1_code"] = data.apply(
        lambda x: x["subregion1_code"]
        if isna(x["subregion2_code"])
        else str(x["subregion2_code"])[:2],
        axis=1,
    )
    return data


class CzechRepublicL1TestedDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(dataframes[0], _column_adapter, drop=True)
        data["key"] = "CZ"
        return data


class CzechRepublicL3DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(dataframes[0], _column_adapter, drop=True)
        data = _parse_region_codes(data)
        return _aggregate_regions(data, ["date", "subregion1_code"])


class CzechRepublicAgeSexDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename appropriate columns
        data = table_rename(dataframes[0], _column_adapter)
        data = _parse_region_codes(data).dropna(subset=["date"])

        # Create stratified age bands
        data.age = data.age.apply(age_group)

        # Rename the sex values
        data.sex = data.sex.apply({"M": "male", "Z": "female"}.get)

        # Go from individual case records to key-grouped records in a flat table
        data[parse_opts["column_name"]] = 1
        data = data.groupby(["date", "subregion1_code", "subregion2_code", "age", "sex"]).sum()

        # Aggregate L2 + L3 data
        return _aggregate_regions(data.reset_index(), ["date", "subregion1_code", "age", "sex"])
