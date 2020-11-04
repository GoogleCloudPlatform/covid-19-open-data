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
from lib.data_source import DataSource
from lib.time import datetime_isoformat
from lib.utils import table_rename, pivot_table


class ISCIIIConfirmedDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        confirmed = table_rename(
            dataframes["confirmed"],
            {
                "ccaa_iso": "subregion1_code",
                "fecha": "date",
                "num_casos_prueba_pcr": "new_confirmed",
            },
            drop=True,
        )

        # Convert dates to ISO format
        confirmed["date"] = confirmed["date"].astype(str)

        # Add the country code to all records and declare matching as subregion1
        confirmed["country_code"] = "ES"
        confirmed["subregion2_code"] = None
        confirmed["locality_code"] = None

        # Output the results
        return confirmed


class MSCBSDeceasedDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        deceased = table_rename(dataframes["deceased"], {"FECHA / CCAA": "date"})
        deceased = pivot_table(
            deceased.set_index("date"), value_name="new_deceased", pivot_name="match_string"
        )

        # Convert dates to ISO format
        deceased["date"] = deceased["date"].apply(lambda x: str(x)[:10])
        deceased["date"] = deceased["date"].apply(lambda x: datetime_isoformat(x, "%Y-%m-%d"))

        # Add the country code to all records and declare matching as subregion1
        deceased["country_code"] = "ES"
        deceased["subregion2_code"] = None
        deceased["locality_code"] = None

        # Country level is declared as "espana"
        deceased["key"] = None
        deceased.loc[deceased["match_string"] == "espana", "key"] = "ES"

        # Output the results
        return deceased.dropna(subset=["date"])
