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

import re
from typing import Any, Dict
from pandas import DataFrame
from lib.data_source import DataSource


class GoogleMobilityDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = dataframes[0]

        # Rename mobility columns to add a prefix
        data.columns = [
            f"mobility_{re.sub('_percent.+', '', col)}" if "percent" in col else col
            for col in data.columns
        ]

        # Join with our known list of PlaceIDs
        data = data.dropna(subset=["place_id"])
        place_ids = aux["knowledge_graph"].dropna(subset=["place_id"])
        data = data.merge(place_ids, on="place_id", how="inner")

        return data
