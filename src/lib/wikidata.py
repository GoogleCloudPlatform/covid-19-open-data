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

import traceback
from functools import partial
from time import sleep
from typing import Any, Iterable, List, Tuple

import requests

from lib.concurrent import thread_map
from lib.error_logger import ErrorLogger

_max_retries = 8
_wikidata_url = "https://query.wikidata.org/bigdata/namespace/wdq/sparql"
_default_query = "SELECT ?pid ?prop WHERE {{ ?pid wdt:{prop} ?prop. FILTER (?pid = wd:{entity}) }}"
_request_header = {"User-Agent": "covid-19-open-data/0.0 (linux-gnu)"}


def _query_property(
    prop: str, entity: str, query: str = _default_query, error_logger: ErrorLogger = None
) -> Any:
    # Time to wait before retry in case of failure
    wait_time = 8

    # Build the query from template
    query = query.format(prop=prop, entity=entity)

    # Keep trying request until succeeds, or _max_retries is reached
    for i in range(_max_retries):
        response = None

        try:
            params = {"query": query, "format": "json"}
            response = requests.get(_wikidata_url, headers=_request_header, params=params)
            data = response.json()

            # Return the first binding available (there should be only one)
            for item in data["results"]["bindings"]:
                return item["prop"]["value"]

        except Exception as exc:
            # If limit is reached, then log error
            if i + 1 < _max_retries:
                if error_logger is not None:
                    error_logger.errlog(response.text if response is not None else exc)
                else:
                    traceback.print_exc()

            # Otherwise use exponential backoff in case of error
            else:
                sleep(wait_time)
                wait_time *= 2

    return None


def wikidata_property(
    prop: str,
    entities: List[str],
    query: str = _default_query,
    error_logger: ErrorLogger = None,
    **tqdm_kwargs
) -> Iterable[Tuple[str, Any]]:
    """
    Query a single property from Wikidata, and return all entities which are part of the provided
    list which contain that property.

    Arguments:
        prop: Wikidata property, for example P1082 for population.
        entities: List of Wikidata identifiers to query the desired property.
        query: [Optional] SPARQL query used to retrieve `prop`.
        error_logger: [Optional] ErrorLogger instance to use for logging.
    Returns:
        Iterable[Tuple[str, Any]]: Iterable of <Wikidata ID, property value>
    """
    # Limit parallelization to avoid hitting rate limits
    tqdm_kwargs["max_workers"] = 6
    map_func = partial(_query_property, prop, query=query, error_logger=error_logger)
    for entity, prop in zip(entities, thread_map(map_func, entities, **tqdm_kwargs)):
        yield entity, prop
