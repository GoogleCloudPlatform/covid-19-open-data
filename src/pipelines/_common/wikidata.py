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
from typing import Dict, Tuple

from pandas import DataFrame

from lib.data_source import DataSource
from lib.wikidata import wikidata_properties
from lib.concurrent import thread_map


class WikidataDataSource(DataSource):
    """ Retrieves the requested properties from Wikidata for all items in metadata.csv """

    @staticmethod
    def _process_item(props: Dict[str, str], key_wikidata: Tuple[str, str]):
        key, wikidata_id = key_wikidata
        return {"key": key, **wikidata_properties(props, wikidata_id)}

    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        data = aux["knowledge_graph"].merge(aux["metadata"])[["key", "wikidata"]].set_index("key")

        # Load wikidata using parallel processing
        map_iter = data.wikidata.iteritems()
        map_func = partial(self._process_item, parse_opts)
        records = thread_map(map_func, map_iter, total=len(data), desc="Wikidata Properties")

        # Return all records in DataFrame form
        return DataFrame.from_records(records)
