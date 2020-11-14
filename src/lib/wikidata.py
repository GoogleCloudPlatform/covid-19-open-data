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

import time
import traceback
from typing import Any, List

import requests

from lib.error_logger import ErrorLogger
from lib.io import pbar

_LIMIT = 500_000
_MAX_RETRIES = 4
_INIT_WAIT_TIME = 8
_WD_TIMEOUT = 60 * 5
_WD_URL = "http://query.wikidata.org/bigdata/namespace/wdq/sparql"
_WD_QUERY = "SELECT ?pid ?prop WHERE {{ ?pid wdt:{prop} ?prop. }}"
_REQUEST_HEADER = {"User-Agent": "covid-19-open-data/0.0 (linux-gnu)"}


def wikidata_property(
    prop: str,
    entities: List[str],
    query_template: str = _WD_QUERY,
    logger: ErrorLogger = ErrorLogger(),
    offset: int = 0,
    **tqdm_kwargs,
) -> Any:
    """
    Query a single property from Wikidata, and return all entities which are part of the provided
    list which contain that property.

    Arguments:
        prop: Wikidata property, for example P1082 for population.
        entities: List of Wikidata identifiers to query the desired property.
        query: [Optional] SPARQL query used to retrieve `prop`.
        logger: [Optional] ErrorLogger instance to use for logging.
        offset: [Optional] Number of items to skip in the result set.
    Returns:
        Iterable[Tuple[str, Any]]: Iterable of <Wikidata ID, property value>
    """
    # Time to wait before retry in case of failure
    wait_time = _INIT_WAIT_TIME

    # Build the query from template
    tpl = query_template + " LIMIT {limit} OFFSET {offset}"
    query = tpl.format(prop=prop, limit=_LIMIT, offset=offset)

    # Keep trying request until succeeds, or _max_retries is reached
    for i in range(_MAX_RETRIES):
        response = None

        try:
            start_time = time.monotonic()
            params = {"query": query, "format": "json"}
            req_opts = dict(headers=_REQUEST_HEADER, params=params, timeout=_WD_TIMEOUT)
            response = requests.get(_WD_URL, **req_opts)
            elapsed_time = time.monotonic() - start_time
            log_opts = dict(status=response.status_code, url=_WD_URL, time=elapsed_time, **params)
            logger.log_info(f"Wikidata SPARQL server response", **log_opts)
            data = response.json()

            # Return the first binding available (there should be only one)
            for item in pbar(data["results"]["bindings"], **tqdm_kwargs):
                pid = item["pid"]["value"].split("/")[-1]
                if pid in entities:
                    yield pid, item["prop"]["value"]

            # Unless we got `_LIMIT` results, keep adding offset until we run our of results
            if len(data["results"]["bindings"]) == _LIMIT:
                yield from wikidata_property(
                    prop,
                    entities,
                    query_template=query_template,
                    logger=logger,
                    offset=offset + _LIMIT,
                    **tqdm_kwargs,
                )

            # If no exceptions were thrown, we have reached the end
            logger.log_info(f"Wikidata SPARQL results end reached")
            return

        except Exception as exc:

            # If we have reached the error limit, log and re-raise the error
            if i + 1 >= _MAX_RETRIES:
                msg = response.text if response is not None else "Unknown error"
                logger.log_error(msg, exc=exc, traceback=traceback.format_exc())
                raise exc

            # Use exponential backoff in case of error
            logger.log_info(f"({i + 1}) Request error. Retry in {wait_time} seconds...", exc=exc)
            time.sleep(wait_time)
            wait_time *= 2
