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
from pandas import DataFrame
from lib.pipeline import DataSource
from lib.utils import grouped_diff


class Covid19AuDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = dataframes[0].transpose()

        # Transform the data from non-tabulated format to record format
        records = []
        for idx, row in data.iterrows():
            for code in data.columns:
                subset = row[code]
                record = {
                    "date": idx.date().isoformat(),
                    "country_code": "AU",
                    "subregion1_code": code,
                    "confirmed": subset[0],
                }
                if len(subset) > 1:
                    record["deceased"] = subset[1]
                if len(subset) > 2:
                    record["recovered"] = subset[2]
                if len(subset) > 3:
                    record["tested"] = subset[3]
                records.append(record)

        data = DataFrame.from_records(records)
        return grouped_diff(data, ["country_code", "subregion1_code", "date"])
