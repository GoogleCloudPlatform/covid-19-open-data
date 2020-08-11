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

import json
from datetime import datetime
from typing import Dict

from pandas import DataFrame

from lib.utils import table_multimerge
from pipelines.epidemiology.es_cn_authority import CanaryIslandsDataSource


class CanaryIslandsHospitalizationsDataSource(CanaryIslandsDataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        with open(sources[0], "r") as fd:
            features = json.load(fd)["features"]

        records = {"hospitalized": [], "intensive_care": [], "ventilator": []}
        for record in features:

            if record["SERIE"] == "HPT":
                statistic = "hospitalized"
            elif record["SERIE"] == "CSR":
                statistic = "intensive_care"
            elif record["SERIE"] == "CCR":
                statistic = "ventilator"
            else:
                self.errlog(f"Unknown statistic type: {statistic}")
                continue
            records[statistic].append(
                {
                    "date": datetime.fromtimestamp(record["FECHA"] / 1000).date().isoformat(),
                    f"current_{statistic}": record["CV19"],
                }
            )

        dataframes = []
        for df in records.values():
            dataframes.append(DataFrame.from_records(df).groupby("date").sum().reset_index())

        data = table_multimerge(dataframes, how="outer")
        data["key"] = "ES_CN"
        return data
