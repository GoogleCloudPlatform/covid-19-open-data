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
from functools import partial
from typing import Dict, Iterable, List, Tuple

from pandas import DataFrame

from lib.concurrent import thread_map
from lib.wikidata import wikidata_property
from pipelines._common.wikidata import WikidataDataSource


def _extract_coordinates(results: Iterable[Tuple[str, str]]) -> Iterable[Tuple[str, str]]:
    for pid, prop in results:
        coord_search = re.search(r"Point\((.+)\)", prop, re.IGNORECASE)
        if coord_search:
            # Latitude and longitude are in reverse order for Point objects
            lon, lat = str(coord_search.group(1)).split(" ")
            yield (pid, lat, lon)


class WikidataCoordinatesDataSource(WikidataDataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        data = aux["knowledge_graph"].merge(aux["metadata"])[["key", "wikidata"]]
        entities = data.dropna().set_index("wikidata")

        # Load wikidata using parallel processing
        wikidata_props = {v: k for k, v in parse_opts.items()}
        map_func = partial(self._process_item, entities.index)
        map_opts = dict(desc="Wikidata Properties", total=len(wikidata_props))
        for _, values in thread_map(map_func, wikidata_props.keys(), **map_opts):
            values = _extract_coordinates(values)
            df = DataFrame.from_records(values, columns=["wikidata", "latitude", "longitude"])
            entities = entities.join(df.set_index("wikidata"), how="outer")

        # Return all records in DataFrame form
        return entities
