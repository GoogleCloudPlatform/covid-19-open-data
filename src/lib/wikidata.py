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

from typing import Any, Iterable, List, Tuple
import requests


def _query_property(prop: str) -> Iterable[Tuple[str, Any]]:
    url = "https://query.wikidata.org/bigdata/namespace/wdq/sparql"
    query = f"SELECT ?pid ?prop WHERE {{ ?pid wdt:{prop} ?prop }}"
    data = requests.get(url, params={"query": query, "format": "json"}).json()
    for item in data["results"]["bindings"]:
        yield item["pid"]["value"].split("/")[-1], item["prop"]["value"]


def wikidata_property(prop: str, entities: List[str]) -> Iterable[Tuple[str, Any]]:
    """
    Query a single property from Wikidata, and return all entities which are part of the provided
    list which contain that property.

    Arguments:
        prop: Wikidata property, for example P1082 for population
        entities: List of Wikidata identifiers to query the desired property
    Returns:
        Iterable[Tuple[str, Any]]: Iterable of <Wikidata ID, property value>
    """
    properties = {}
    entities_set = set(entities)
    values = filter(lambda x: x[0] in entities_set, _query_property(prop))
    for entity, prop_value in values:
        properties[entity] = prop_value

    for entity in entities:
        yield entity, properties.get(entity)
