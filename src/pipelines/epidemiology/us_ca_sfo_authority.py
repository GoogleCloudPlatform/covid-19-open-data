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
from lib.utils import table_rename, table_merge

# specimen_collection_date,tests,pos,pct,neg,indeterminate,Last Updated At
_column_adapter = {
    "specimen_collection_date": "date",
    "Specimen Collection Date": "date",
    "Case Count": "new_confirmed",
    "tests": "new_tested",
}


class SanFranciscoDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        tables = [table_rename(table, _column_adapter, drop=True) for table in dataframes.values()]
        data = table_merge(tables, on="date", how="outer")

        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%Y/%m/%d"))
        data["key"] = "US_CA_SFO"
        return data
