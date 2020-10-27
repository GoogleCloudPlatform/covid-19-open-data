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
from typing import Dict
from pandas import DataFrame
from lib.data_source import DataSource


class RomaniaDataSource(DataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        with open(sources[0]) as fd:
            data = json.load(fd)
        rows = [data["currentDayStats"]] + list(data["historicalData"].values())

        records = []
        for row in rows:
            records.append(
                {
                    "key": "RO",
                    "date": row["parsedOnString"],
                    "total_confirmed": row["numberInfected"],
                    "total_deceased": row["numberDeceased"],
                    "total_recovered": row["numberCured"],
                    "total_confirmed_age_00": row["distributionByAge"].get("0-9"),
                    "total_confirmed_age_01": row["distributionByAge"].get("10-19"),
                    "total_confirmed_age_02": row["distributionByAge"].get("20-29"),
                    "total_confirmed_age_03": row["distributionByAge"].get("30-39"),
                    "total_confirmed_age_04": row["distributionByAge"].get("40-49"),
                    "total_confirmed_age_05": row["distributionByAge"].get("50-59"),
                    "total_confirmed_age_06": row["distributionByAge"].get("60-69"),
                    "total_confirmed_age_07": row["distributionByAge"].get("70-79"),
                    "total_confirmed_age_08": row["distributionByAge"].get(">80"),
                    "age_bin_00": "00-09",
                    "age_bin_01": "10-19",
                    "age_bin_02": "20-29",
                    "age_bin_03": "30-39",
                    "age_bin_04": "40-49",
                    "age_bin_05": "50-59",
                    "age_bin_06": "60-69",
                    "age_bin_07": "70-79",
                    "age_bin_08": "80-",
                }
            )
            if not parse_opts.get("skip_l2"):
                for code, num in row.get("countyInfectionsNumbers", {}).items():
                    if code != "-":
                        records.append(
                            {
                                "key": f"RO_{code}",
                                "date": row["parsedOnString"],
                                "total_confirmed": num,
                            }
                        )

        return DataFrame.from_records(records)
