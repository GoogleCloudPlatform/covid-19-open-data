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

import json
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List

import requests
from pandas import DataFrame

from lib.data_source import DataSource


def _download_arcgis(
    url: str, offset: int = 0, log_func: Callable[[str], None] = None
) -> List[Dict[str, Any]]:
    """
    Recursively download all records from an ArcGIS data source respecting the maximum record
    transfer per request.
    """
    url_tpl = url + "&resultOffset={offset}"

    try:
        res = requests.get(url_tpl.format(offset=offset)).json()["features"]
    except Exception as exc:
        if log_func:
            log_func(requests.get(url_tpl.format(offset=offset)).text)
        raise exc

    rows = [row["attributes"] for row in res]
    if len(rows) == 0:
        return rows
    else:
        return rows + _download_arcgis(url, offset=offset + len(rows))


class ArcGISDataSource(DataSource):
    def fetch(
        self, output_folder: Path, cache: Dict[str, str], fetch_opts: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        downloaded_files = {}

        for idx, opts in enumerate(fetch_opts):
            # Base URL comes from fetch_opts
            url_base = opts["url"]

            # Create a deterministic file name
            file_path = (
                output_folder
                / "snapshot"
                / ("%s.%s" % (uuid.uuid5(uuid.NAMESPACE_DNS, url_base), "json"))
            )

            # Avoid download if the file exists and flag is set
            skip_existing = opts.get("opts", {}).get("skip_existing")
            if not skip_existing or not file_path.exists():
                with open(file_path, "w") as fd:
                    json.dump({"features": _download_arcgis(url_base)}, fd)

            # Add downloaded file to the list
            downloaded_files[opts.get("name", idx)] = str(file_path.absolute())

        return downloaded_files

    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        dataframes = {}
        for name, file_path in sources.items():
            with open(file_path, "r") as fd:
                records = json.load(fd)["features"]
                dataframes[name] = DataFrame.from_records(records)
        return self.parse_dataframes(dataframes, aux, **parse_opts)
