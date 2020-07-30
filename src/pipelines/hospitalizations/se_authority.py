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
from lib.io import read_file
from lib.data_source import DataSource
from lib.time import datetime_isoformat


class SwedenDataSource(DataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        data = read_file(sources[0], sheet_name="Antal intensivvårdade per dag").rename(
            columns={"Datum_vårdstart": "date", "Antal_intensivvårdade": "new_intensive_care"}
        )

        # Get date in ISO format
        data["key"] = "SE"
        # The source is actually %m/%d/%Y but pandas silently converts it to date object
        data["date"] = data["date"].astype(str).apply(lambda x: datetime_isoformat(x, "%Y-%m-%d"))
        return data
