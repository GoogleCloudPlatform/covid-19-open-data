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

from typing import Any, Dict, List, Tuple
from pandas import DataFrame, Int64Dtype, merge
from lib.pipeline import DataSource, DataSource, DataPipeline
from lib.utils import ROOT


class IndexDataSource(DataSource):
    def parse(self, sources: List[str], aux: Dict[str, DataFrame], **parse_opts):
        data = aux["metadata"].merge(aux["knowledge_graph"], how="left")

        # Country codes are joined by country_code rather than the usual key
        country_codes = aux["country_codes"].rename(columns={"key": "country_code"})
        data = merge(data, country_codes, how="left")

        # Determine the level of aggregation for each datapoint
        data["aggregation_level"] = None
        subregion1_null = data.subregion1_code.isna()
        subregion2_null = data.subregion2_code.isna()
        data.loc[subregion1_null & subregion2_null, "aggregation_level"] = 0
        data.loc[~subregion1_null & subregion2_null, "aggregation_level"] = 1
        data.loc[~subregion1_null & ~subregion2_null, "aggregation_level"] = 2
        return data
