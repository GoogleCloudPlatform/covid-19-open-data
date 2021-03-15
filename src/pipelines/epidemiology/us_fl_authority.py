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
import datetime
from typing import Any, Dict

from pandas import DataFrame, concat

from lib.arcgis_data_source import ArcGISDataSource
from lib.case_line import convert_cases_to_time_series
from lib.cast import safe_int_cast

fromtimestamp = datetime.datetime.fromtimestamp


class FloridaDataSource(ArcGISDataSource):
    def parse(self, sources: Dict[Any, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        with open(sources[0], "r") as fd:
            records = json.load(fd)["features"]

        cases = DataFrame.from_records(records)
        cases["date_new_confirmed"] = cases["ChartDate"].apply(
            lambda x: fromtimestamp(x // 1000).date().isoformat()
        )

        # FL does not provide date for deceased or hospitalized, so we just copy it from confirmed
        deceased_mask = cases.Died.str.lower() == "yes"
        hospitalized_mask = cases.Hospitalized.str.lower() == "yes"
        cases["date_new_deceased"] = None
        cases["date_new_hospitalized"] = None
        cases.loc[deceased_mask, "date_new_deceased"] = cases.loc[
            deceased_mask, "date_new_confirmed"
        ]
        cases.loc[hospitalized_mask, "date_new_hospitalized"] = cases.loc[
            hospitalized_mask, "date_new_confirmed"
        ]

        # Rename the sex labels
        sex_adapter = lambda x: {"male": "male", "female": "female"}.get(x, "sex_unknown")
        cases["sex"] = cases["Gender"].str.lower().apply(sex_adapter)
        cases.drop(columns=["Gender"], inplace=True)

        # Make sure age is an integer
        cases["age"] = cases["Age"].apply(safe_int_cast)
        cases.drop(columns=["Age"], inplace=True)

        cases = cases.rename(columns={"County": "match_string"})
        data = convert_cases_to_time_series(cases, ["match_string"])
        data["country_code"] = "US"
        data["subregion1_code"] = "FL"

        # Aggregate to state level here, since some data locations are "Unknown"
        group_cols = ["country_code", "subregion1_code", "date", "age", "sex"]
        state = data.drop(columns=["match_string"]).groupby(group_cols).sum().reset_index()

        # Remove bogus data
        data = data[data.match_string != "Unknown"]

        return concat([state, data])
