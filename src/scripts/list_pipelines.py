#!/usr/bin/env python
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

# A script to detect all pipelines declared in pipelines/ and cache/ and
# dump a summary of their configurations within a `tmp/` folder in the
# project root.
#
# Example usage: `python src/scripts/list_pipelines.py`

import sys
import os
import json

path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(path)

from pandas import DataFrame
from lib.constants import SRC
from lib.pipeline import DataPipeline
from typing import List, Iterator, Dict


def get_pipeline_names() -> Iterator[str]:
    for item in (SRC / "pipelines").iterdir():
        if not item.name.startswith("_") and not item.is_file():
            yield item.name


def get_source_configs(pipeline_names: List[str]) -> Iterator[Dict]:
    """Map a list of pipeline names to their source configs."""

    for pipeline_name in pipeline_names:
        data_pipeline = DataPipeline.load(pipeline_name)

        for data_source in data_pipeline.data_sources:
            data_source_config = data_source.config

            data_source_name = data_source_config.get("name")
            data_source_fetch_params = data_source_config.get("fetch", [])

            for fetch_param in data_source_fetch_params:
                file_ext = fetch_param.get("opts", {}).get("ext", "")
                data_source_url = fetch_param.get("url")

                yield {
                    "pipeline_name": pipeline_name,
                    "source_name": data_source_name,
                    "url": data_source_url,
                    "ext": file_ext,
                }


def get_cache_configs() -> Iterator[Dict]:
    """Generate a list of configurations for cached data sources."""

    config_json = SRC / "cache" / "config.json"
    with open(config_json, "r") as fd:
        cache_pipelines_config = json.load(fd)
        for pipeline_config in cache_pipelines_config:
            yield {"url": pipeline_config.get("url"), "cmd": pipeline_config.get("cmd")}


if __name__ == "__main__":
    source_configs_df = DataFrame(get_source_configs(get_pipeline_names()))
    cache_configs_df = DataFrame(get_cache_configs())

    print(source_configs_df)
    print(cache_configs_df)

    source_configs_df.to_csv(SRC / ".." / "tmp" / "source_configs.csv")
    cache_configs_df.to_csv(SRC / ".." / "tmp" / "cache_configs.csv")
