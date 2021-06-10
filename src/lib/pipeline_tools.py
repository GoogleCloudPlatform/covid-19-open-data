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

from typing import Iterable, Dict
import yaml
from .constants import SRC, OUTPUT_COLUMN_ADAPTER
from .pipeline import DataPipeline, DataSource


def get_pipeline_names() -> Iterable[str]:
    """ Iterable with all the names of available data pipelines """
    for item in sorted((SRC / "pipelines").iterdir()):
        if not item.name.startswith("_") and not item.is_file():
            yield item.name


def get_table_names() -> Iterable[str]:
    """ Iterable with all the available table names """
    for pipeline_name in get_pipeline_names():
        yield pipeline_name.replace("_", "-")


def get_pipelines() -> Iterable[DataPipeline]:
    """ Iterable with all the available data pipelines """
    for pipeline_name in get_pipeline_names():
        yield DataPipeline.load(pipeline_name)


def get_schema() -> Dict[str, type]:
    """ Outputs all known column schemas """
    schema: Dict[str, type] = {}

    # Add all columns from pipeline configs
    for pipeline in get_pipelines():
        schema.update(pipeline.schema)

    # Add new columns from adapter
    for col_old, col_new in OUTPUT_COLUMN_ADAPTER.items():
        if col_old in schema and col_new is not None:
            schema[col_new] = schema[col_old]

    return schema


def iter_data_sources():
    for name in get_pipeline_names():
        config_path = SRC / "pipelines" / name / "config.yaml"
        with open(config_path, "r") as fd:
            config_yaml = yaml.safe_load(fd)
            for source_config in config_yaml["sources"]:
                yield name, DataSource(source_config)
