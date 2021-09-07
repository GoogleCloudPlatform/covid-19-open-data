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

from typing import Dict

from pandas import DataFrame, melt

from lib.data_source import DataSource
from lib.time import datetime_isoformat
from lib.utils import table_merge, table_rename
from lib.metadata_utils import country_subregion1s


_column_adapter = {
    "Vaccinated As of": "date",
    "State": "subregion1_name",
    "First Dose Administered": "total_persons_vaccinated",
    "Second Dose Administered": "total_persons_fully_vaccinated",
    "Total Doses Administered": "total_vaccine_doses_administered",
}


class Covid19IndiaOrgL1DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = table_rename(dataframes[0], _column_adapter, drop=True)
        data.date = data.date.apply(lambda x: datetime_isoformat(x, "%d/%m/%Y"))
        # add location keys
        subregion1s = country_subregion1s(aux["metadata"], "IN")
        data = table_merge(
            [data, subregion1s[["key", "subregion1_name"]]], on=["subregion1_name"], how="inner"
        )
        return data
