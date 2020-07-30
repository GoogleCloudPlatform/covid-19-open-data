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
from lib.case_line import convert_cases_to_time_series

from lib.data_source import DataSource
from lib.time import datetime_isoformat
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
    data = data[data["subregion2_code"] != ""].copy()

    l2["key"] = "CZ_" + l2["subregion1_code"]
    data["key"] = "CZ_" + data["subregion1_code"] + "_" + data["subregion2_code"]

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
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(dataframes[0], _column_adapter, drop=True)
        data["key"] = "CZ"
        return data


class CzechRepublicL3DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(dataframes[0], _column_adapter, drop=True)
        data = _parse_region_codes(data)
        return _aggregate_regions(data, ["date", "subregion1_code"])


class CzechRepublicAgeSexDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename appropriate columns
        col = parse_opts["column_name"]
        cases = table_rename(dataframes[0], _column_adapter)
        cases = cases.rename(columns={"date": f"date_{col}"})
        cases = _parse_region_codes(cases).dropna(subset=[f"date_{col}"])

        # Rename the sex values
        cases["sex"] = cases["sex"].apply({"M": "male", "Z": "female"}.get)

        # Go from individual case records to key-grouped records in a flat table
        data = convert_cases_to_time_series(
            cases, index_columns=["subregion1_code", "subregion2_code"]
        )

        # Make sure the region codes are strings before parsing them
        data["subregion1_code"] = data["subregion1_code"].astype(str)
        data["subregion2_code"] = data["subregion2_code"].astype(str)

        # Aggregate L2 + L3 data
        data = _aggregate_regions(data, ["date", "subregion1_code", "age", "sex"])

        # Remove bogus values
        data = data[data["key"] != "CZ_99"]
        data = data[data["key"] != "CZ_99_99Y"]

        # Convert all dates to ISO format
        data["date"] = (
            data["date"]
            .astype(str)
            .apply(lambda x: datetime_isoformat(x, "%d.%m.%Y" if "." in x else "%Y-%m-%d"))
        )

        return data
