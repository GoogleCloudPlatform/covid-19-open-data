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

from typing import Any, Dict, List
from pandas import DataFrame, concat, merge
from lib.io import read_file
from lib.pipeline import DataSource
from lib.time import datetime_isoformat
from lib.utils import table_rename


class OpenCovidDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = table_rename(
            dataframes[0],
            parse_opts.get(
                "column_adapter",
                {
                    "discharged_cumulative": "total_discharged",
                    "hospitalized_current": "current_hospitalized",
                    "number hospitalised": "current_hospitalized",
                    "hospitalized_cumulative": "total_hospitalized",
                    "icu_current": "current_intensive_care",
                    "number in icu": "current_intensive_care",
                    "icu_cumulative": "cumulative_intensive_care",
                    "ventilator_current": "current_ventilator",
                    "ventilator_cumulative": "cumulative_ventilator",
                    "new hospital admissions": "new_hospitalized",
                    "new intensive care admissions": "new_intensive_care",
                },
            ),
        )

        # Add key and parse date in ISO format
        data["key"] = parse_opts.get("key")
        data["date"] = data[parse_opts.get("date_column", "date")].astype(str)
        date_format = parse_opts.get("date_format", "%Y-%m-%d")
        data.date = data.date.apply(lambda x: datetime_isoformat(x, date_format))

        return data
