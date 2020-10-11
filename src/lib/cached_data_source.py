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

import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, List
from pandas import DataFrame, concat
from lib.constants import CACHE_URL, READ_OPTS
from lib.data_source import DataSource
from lib.io import read_file
from lib.net import parallel_download


def _generate_snapshot_paths(
    output_folder: Path, url_list: List[str], **download_opts
) -> Iterable[Path]:

    # Create the snapshots folder if it does not exist
    snapshot_folder = output_folder / "snapshot"
    snapshot_folder.mkdir(parents=True, exist_ok=True)

    # Create a deterministic file name for each of the URLs
    for url in url_list:
        ext = url.split(".")[-1]
        yield snapshot_folder / ("%s.%s" % (uuid.uuid5(uuid.NAMESPACE_DNS, url), ext))


class CachedDataSource(DataSource):
    def fetch(
        self, output_folder: Path, cache: Dict[str, str], fetch_opts: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        sources = {}

        for opts in fetch_opts:
            cache_key = opts["cache_key"]

            # Aggregate the cache items by date, since they are downloaded hourly
            cache_urls = {}
            for url_path in cache[cache_key]:
                url_path = url_path.replace("cache/", "")
                date = url_path.split("/", 1)[0]
                cache_urls[date[:10]] = f"{CACHE_URL}/{url_path}"

            # Download the cache items into the snapshots directory
            file_paths = list(_generate_snapshot_paths(output_folder, cache_urls.values()))
            list(parallel_download(list(cache_urls.values()), file_paths))
            sources[cache_key] = {
                date: file_path for date, file_path in zip(cache_urls.keys(), file_paths)
            }

        return sources

    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        read_opts = {k: v for k, v in parse_opts.items() if k in READ_OPTS}

        dataframes = {}
        date_start = parse_opts.pop("date_start", None)
        date_end = parse_opts.pop("date_end", None)
        for cache_key, cache_urls in sources.items():

            daily_data = []
            for date, url in cache_urls.items():
                if date_start is not None and date < date_start:
                    continue
                if date_end is not None and date > date_end:
                    continue

                data = read_file(url, **read_opts)
                data["date"] = date
                daily_data.append(data)

            dataframes[cache_key] = concat(daily_data)

        return self.parse_dataframes(dataframes, aux, **parse_opts)
