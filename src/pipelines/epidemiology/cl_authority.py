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
from lib.cast import numeric_code_as_string
from lib.pipeline import DataSource
from lib.utils import table_rename, table_multimerge

_SUBREGION1_CODE_MAP = {
    "11": "AI",
    "02": "AN",
    "15": "AP",
    "09": "AR",
    "03": "AT",
    "08": "BI",
    "04": "CO",
    "06": "LI",
    "10": "LL",
    "14": "LR",
    "12": "MA",
    "07": "ML",
    "16": "NB",
    "13": "RM",
    "01": "TA",
    "05": "VS",
}


def _extract_cities(data: DataFrame) -> DataFrame:
    # TODO: extract cities
    return DataFrame(columns=data.columns)


class ChileRegionsDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = table_multimerge(
            [
                table_rename(
                    dataframes["confirmed"],
                    {"Fecha": "date", "Total": "new_confirmed", "Region": "match_string"},
                    drop=True,
                ),
                table_rename(
                    dataframes["deceased"],
                    # The file name indicates the counts are cumulative, but they are not
                    {"Fecha": "date", "Total": "total_deceased", "Region": "match_string"},
                    drop=True,
                ),
                table_rename(
                    dataframes["tested"],
                    {"Fecha": "date", "numero": "new_tested", "Region": "match_string"},
                    drop=True,
                ),
            ]
        )

        # Convert date to ISO format
        data["date"] = data["date"].astype(str)

        # Extract cities from the regions
        city = _extract_cities(data)

        # Make sure all records have country code and no subregion code
        data["country_code"] = "CL"
        data["subregion2_code"] = None

        # Drop bogus records from the data
        data.dropna(subset=["date", "match_string"], inplace=True)

        return concat([data, city])


class ChileMunicipalitiesDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = table_multimerge(
            [
                table_rename(
                    dataframes["confirmed"],
                    {
                        "Fecha": "date",
                        "Casos confirmados": "new_confirmed",
                        "Codigo region": "subregion1_code",
                        "Codigo comuna": "subregion2_code",
                    },
                    drop=True,
                ),
                table_rename(
                    dataframes["deceased"],
                    {
                        "Fecha": "date",
                        "Casos fallecidos": "total_deceased",
                        "Codigo region": "subregion1_code",
                        "Codigo comuna": "subregion2_code",
                    },
                    drop=True,
                ),
            ]
        )

        # Convert date to ISO format
        data["date"] = data["date"].astype(str)

        # Parse region codes as strings
        data["subregion1_code"] = data["subregion1_code"].apply(
            lambda x: numeric_code_as_string(x, 2)
        )
        data["subregion2_code"] = data["subregion2_code"].apply(
            lambda x: numeric_code_as_string(x, 5)
        )

        # Use proper ISO codes for the subregion1 level
        data["subregion1_code"] = data["subregion1_code"].apply(_SUBREGION1_CODE_MAP.get)

        # Extract cities from the municipalities
        city = _extract_cities(data)

        # We can build the key for the data directly from the subregion codes
        data["key"] = "CL_" + data["subregion1_code"] + "_" + data["subregion2_code"]

        # Drop bogus records from the data
        data.dropna(subset=["subregion1_code", "subregion2_code"], inplace=True)

        return concat([data, city])
