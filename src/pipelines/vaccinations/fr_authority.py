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

from typing import Any, Dict
from pandas import DataFrame
from lib.cast import numeric_code_as_string
from lib.data_source import DataSource
from lib.time import datetime_isoformat
from lib.utils import aggregate_admin_level, table_rename


_column_adapter = {
    "reg": "subregion1_code",
    "dep": "subregion2_code",
    "jour": "date",
    "n_dose1": "new_persons_vaccinated",
    "n_cum_dose1": "total_persons_vaccinated",
    "n_tot_dose1": "total_persons_vaccinated",
    "n_dose2": "new_persons_fully_vaccinated",
    "n_cum_dose2": "total_persons_fully_vaccinated",
    "n_tot_dose2": "total_persons_fully_vaccinated",
}

_region_code_map = {
    84: "ARA",
    27: "BFC",
    53: "BRE",
    94: "COR",
    24: "CVL",
    44: "GES",
    3: "GF",
    1: "GUA",
    32: "HDF",
    11: "IDF",
    4: "LRE",
    6: "MAY",
    2: "MQ",
    75: "NAQ",
    28: "NOR",
    76: "OCC",
    93: "PAC",
    52: "PDL",
}


def _preprocess_dataframe(data: DataFrame) -> DataFrame:
    data = data.copy()

    # Convert date to ISO format
    data["date"] = data["date"].str.slice(0, 10)

    # In some datasets the second dose column can be missing
    if "new_persons_fully_vaccinated" not in data.columns:
        data["new_persons_fully_vaccinated"] = None
    if "total_persons_fully_vaccinated" not in data.columns:
        data["total_persons_fully_vaccinated"] = None

    # Estimate the doses from person counts
    data["new_vaccine_doses_administered"] = (
        data["new_persons_vaccinated"] + data["new_persons_fully_vaccinated"]
    )
    data["total_vaccine_doses_administered"] = (
        data["total_persons_vaccinated"] + data["total_persons_fully_vaccinated"]
    )

    return data


class FranceDepartmentDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(dataframes[0], _column_adapter, remove_regex=r"[^\w]", drop=True)
        data = _preprocess_dataframe(data)

        # Make sure all records have the country code and match subregion2 only
        data["key"] = None
        data["country_code"] = "FR"
        data["locality_code"] = None

        # We consider some departments as regions
        data.loc[data["subregion2_code"] == "971", "key"] = "FR_GUA"
        data.loc[data["subregion2_code"] == "972", "key"] = "FR_MQ"
        data.loc[data["subregion2_code"] == "973", "key"] = "FR_GF"
        data.loc[data["subregion2_code"] == "974", "key"] = "FR_LRE"

        # Drop bogus data
        data = data[data["subregion2_code"] != "00"]

        return data


class FranceRegionDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(dataframes[0], _column_adapter, remove_regex=r"[^\w]", drop=True)
        data = _preprocess_dataframe(data)

        # Convert the region codes to ISO format
        data["subregion1_code"] = data["subregion1_code"].apply(_region_code_map.get)

        # Make sure all records have the country code and match subregion1 only
        data["country_code"] = "FR"
        data["subregion2_code"] = None
        data["locality_code"] = None

        return data


class FranceCountryDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(dataframes[0], _column_adapter, remove_regex=r"[^\w]", drop=True)
        data = _preprocess_dataframe(data)

        data["key"] = "FR"
        return data
