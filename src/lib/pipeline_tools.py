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

from typing import Iterator, Dict
from .constants import SRC, OUTPUT_COLUMN_ADAPTER_V3
from .pipeline import DataPipeline


def get_pipeline_names() -> Iterator[str]:
    """ Iterator with all the names of available data pipelines """
    for item in sorted((SRC / "pipelines").iterdir()):
        if not item.name.startswith("_") and not item.is_file():
            yield item.name


def get_table_names() -> Iterator[str]:
    """ Iterator with all the available table names """
    for pipeline_name in get_pipeline_names():
        yield pipeline_name.replace("_", "-")


def get_pipelines() -> Iterator[DataPipeline]:
    """ Iterator with all the available data pipelines """
    for pipeline_name in get_pipeline_names():
        yield DataPipeline.load(pipeline_name)


def get_schema(column_adapter: Dict[str, str] = None) -> Dict[str, type]:
    """ Outputs all known column schemas """
    schema: Dict[str, type] = {}

    # Default to using the latest schema
    column_adapter = column_adapter or dict(OUTPUT_COLUMN_ADAPTER_V3)

    # Add all columns from pipeline configs
    for pipeline in get_pipelines():
        schema.update(pipeline.schema)

    # Add new columns from adapter
    for col_old, col_new in column_adapter.items():
        if col_old in schema and col_new is not None:
            schema[col_new] = schema[col_old]

    return schema
