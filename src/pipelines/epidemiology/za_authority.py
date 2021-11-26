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

import json
from datetime import datetime
from typing import Dict

from pandas import DataFrame

from lib.arcgis_data_source import ArcGISDataSource
from lib.utils import table_rename


class SouthAfricaDataSource(ArcGISDataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        with open(sources[0], "r") as fd:
            features = json.load(fd)["features"]

        data = table_rename(
            DataFrame.from_records(features),
            {"Dates": "date", "Daily_Count": "new_confirmed", "Accumulative": "total_confirmed"},
            drop=True,
        )

        # Convert date from timestamp to ISO format
        data["date"] = data["date"].apply(lambda x: datetime.fromtimestamp(x / 1000))
        data["date"] = data["date"].apply(lambda x: x.isoformat()[:10])
        data["key"] = "ZA"

        return data
