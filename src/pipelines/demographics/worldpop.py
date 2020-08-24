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
from lib.data_source import DataSource


class WorldPopPopulationDataSource(DataSource):
    """
    Retrieves demographics information from WorldPop for all items in metadata.csv. The original
    data source is https://www.worldpop.org/project/categories?id=8 but the data is pre-processed
    using Google Earth Engine. See the `earthengine` folder under `src` for more details.
    """

    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        # In this script, we don't have any sources since the Worldpop data is precomputed
        data = aux["worldpop"]

        # Keep only records which are part of the index
        data = data.merge(aux["metadata"][["key"]])

        # Add the `population_` prefix to all column names
        column_map = {col: f"population_{col}" for col in data.columns if col != "key"}
        data = data.rename(columns=column_map)

        # There are several Limas:
        # 1. Lima Province (Metropolitan Municipality of Lima), where the city of Lima is contained
        # 2. Lima Department (Lima Region), adjacent to Lima Province but otherwise unrelated
        # 3. Lima District, which is one of the districts within Lima Province
        # Even though #1 and #2 are disjoint, OpenStreetMap relation ID for #2 contains #1, so we
        # manually subtract the values until the error is corrected upstream which includes
        # conflicting information in Wikipedia, OpenStreetMap and DataCommons.
        lima_province_mask = data["key"] == "PE_LMA"
        lima_department_mask = data["key"] == "PE_LIM"
        for col in column_map.values():
            lima_province_pop = data.loc[lima_province_mask, col].iloc[0]
            lima_department_pop = data.loc[lima_department_mask, col].iloc[0]
            if lima_department_pop > lima_province_pop:
                data.loc[lima_department_mask, col] = lima_department_pop - lima_province_pop

        # WorldPop only provides data for people up to 80 years old, but we want 10-year buckets
        # until 90, and 90+ instead. We estimate that, within the 80+ group, 80% are 80-90 and
        # 20% are 90+. This is based on multiple reports, for example:
        # https://www.statista.com/statistics/281174/uk-population-by-age
        data["population_age_90_99"] = data["population_age_80_and_older"] * 0.2
        data["population_age_80_89"] = data["population_age_80_and_older"] * 0.8

        return data
