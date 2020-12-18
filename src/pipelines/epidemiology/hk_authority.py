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
from lib.case_line import convert_cases_to_time_series
from lib.pipeline import DataSource
from lib.time import datetime_isoformat
from lib.utils import table_rename


class HongKongDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        cases = table_rename(
            dataframes[0],
            {
                # "Case no.": "",
                "Report date": "_date",
                # "Date of onset": "date_onset",
                "Gender": "sex",
                "Age": "age",
                # "Name of hospital admitted": "",
                "Hospitalised/Discharged/Deceased": "_status",
                # "HK/Non-HK resident": "",
                # "Case classification*": "",
                # "Confirmed/probable": "",
            },
            drop=True,
            remove_regex=r"[^0-9a-z\s]",
        )

        # Convert date to ISO format
        cases["_date"] = cases["_date"].apply(lambda x: datetime_isoformat(x, "%d/%m/%Y"))

        # All cases in the data are confirmed (or probable)
        cases["date_new_confirmed"] = cases["_date"]

        # Use confirmed date as estimate for deceased date
        cases["date_new_deceased"] = None
        deceased_mask = cases["_status"] == "Deceased"
        cases.loc[deceased_mask, "date_new_deceased"] = cases.loc[deceased_mask, "_date"]

        # Use confirmed date as estimate for hospitalization date
        cases["date_new_hospitalized"] = None
        hosp_mask = (cases["_status"] == "Discharged") | (cases["_status"] == "Hospitalized")
        cases.loc[hosp_mask, "date_new_hospitalized"] = cases.loc[hosp_mask, "_date"]

        # Translate sex labels; only male, female and unknown are given
        sex_adapter = lambda x: {"M": "male", "N": "female"}.get(x, "sex_unknown")
        cases["sex"] = cases["sex"].apply(sex_adapter)

        cases["key"] = "HK"
        return convert_cases_to_time_series(cases, ["key"])
