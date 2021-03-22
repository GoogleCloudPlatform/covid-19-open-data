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
from lib.utils import table_merge


class Covid19IndiaOrgL1DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = dataframes[0]
        # Flatten the table
        data = melt(data, id_vars=["State"], var_name="date", value_name='total_vaccine_doses_administered')
        data.date = data.date.apply(lambda x: datetime_isoformat(x, "%d/%m/%Y"))
        # add location keys
        subregion1s = aux["metadata"].query('(country_code == "IN") & subregion1_code.notna() & subregion2_code.isna() & locality_code.isna()')
        data = table_merge(
            [data, subregion1s[['key', 'subregion1_name']]],
            left_on="State", right_on='subregion1_name', how="inner")
        return data
