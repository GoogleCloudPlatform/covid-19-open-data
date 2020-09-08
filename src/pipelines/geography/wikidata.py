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

from functools import partial
from typing import Dict, List

from pandas import DataFrame

from lib.data_source import DataSource
from lib.wikidata import wikidata_property
from lib.concurrent import thread_map

_query_string_coordinates = """
SELECT ?pid ?prop WHERE {{
    ?pid p:{prop} ?statement .
    ?statement psv:{prop} ?node .
    ?node wikibase:geoLatitude ?latitude .
    ?node wikibase:geoLongitude ?longitude .
    BIND (CONCAT(STR(?latitude), ",", STR(?longitude)) AS ?prop) .
    FILTER (?pid = wd:{entity})
}}
"""


class WikidataCoordinatesDataSource(DataSource):
    def _process_item(self, entities: List[str], prop: str) -> DataFrame:
        return (
            prop,
            wikidata_property(
                prop,
                entities,
                query=_query_string_coordinates,
                error_logger=self,
                desc="Wikidata Entities",
                max_workers=4,
            ),
        )

    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        data = aux["knowledge_graph"].merge(aux["metadata"])[["key", "wikidata"]]
        entities = data.dropna().set_index("wikidata")

        # Load wikidata using parallel processing
        wikidata_props = {v: k for k, v in parse_opts.items()}
        map_func = partial(self._process_item, entities.index)
        for _, values in thread_map(map_func, wikidata_props.keys(), desc="Wikidata Properties"):
            values = ((x[0], *(x[1].split(",", 2) if x[1] else (None, None))) for x in values)
            df = DataFrame.from_records(values, columns=["wikidata", "latitude", "longitude"])
            entities = entities.join(df.set_index("wikidata"), how="outer")

        # Return all records in DataFrame form
        return entities
