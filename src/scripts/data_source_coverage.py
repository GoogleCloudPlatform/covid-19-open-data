#!/usr/bin/env python
# Copyright 2021 Google LLC
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
import os
import sys
from numpy import DataSource
from pandas import DataFrame
from tqdm import tqdm
from typing import Any, Dict, Iterable, List, Tuple

# Add our library utils to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from lib.constants import SRC, GCS_BUCKET_TEST, GCS_BUCKET_PROD, OUTPUT_COLUMN_ADAPTER
from lib.io import export_csv, read_table, temporary_directory
from lib.gcloud import download_file
from lib.pipeline import DataPipeline
from lib.pipeline_tools import get_pipelines, iter_data_sources


def load_combined_table(pipeline: DataPipeline, prod_folder: str) -> DataFrame:
    table_name = pipeline.table
    with temporary_directory() as workdir:
        output_path = workdir / f"{table_name}.csv"
        download_file(GCS_BUCKET_PROD, f"{prod_folder}/{table_name}.csv", output_path)
        combined_table = read_table(output_path)
        index_columns = (["date"] if "date" in combined_table.columns else []) + ["location_key"]
        return combined_table.set_index(index_columns)


def load_intermediate_tables(
    pipeline: DataPipeline, column_adapter: List[str], index_columns: List[str]
) -> Iterable[Tuple[DataSource, DataFrame]]:
    with temporary_directory() as workdir:

        for data_source in tqdm(pipeline.data_sources, desc="Downloading intermediate tables"):
            fname = data_source.uuid(pipeline.table) + ".csv"
            try:
                download_file(GCS_BUCKET_TEST, f"intermediate/{fname}", workdir / fname)
                table = read_table(workdir / fname).rename(columns=column_adapter)
                table = table.groupby(index_columns).last()
                yield (data_source, table)
            except Exception as exc:
                print(f"intermediate table not found: {fname}", file=sys.stderr)


def map_table_sources_to_index(
    data_sources: Dict[str, int], pipeline: DataPipeline, prod_folder: str = "v3"
):
    """ Publishes a table with the data source of each data point """

    # Filter out data sources from other pipelines
    data_sources = [src for src in data_sources if src["table"] == pipeline.table]

    # Convert the data source list into a map for easy lookup by index
    source_index_map = {src["class"]: src["index"] for src in data_sources}

    # Download the combined table and all the intermediate files used to create it
    combined_table = load_combined_table(pipeline, prod_folder)
    index_columns = combined_table.index.names
    intermediate_tables = list(
        load_intermediate_tables(pipeline, OUTPUT_COLUMN_ADAPTER, index_columns)
    )

    # Iterate over the indices for each column independently
    source_dict = {idx: {} for idx in combined_table.index}
    for data_source, table in tqdm(intermediate_tables, desc="Building index map"):
        if data_source.config["class"] not in source_index_map:
            # Maybe a new data source was recently added and we have not updated the
            # data source dictionary yet
            continue

        for col in table.columns:
            subset = table[[col]].dropna()
            for idx in filter(lambda idx: idx in source_dict, subset.index):
                source_dict[idx][col] = source_index_map[data_source.config["class"]]
        # for idx, row in table.iterrows():
        #     if idx not in source_map:
        #         # If the intermediate table was *just* updated some values may not be found in
        #         # the combined table yet.
        #         continue
        #     for col in filter(lambda col: not isna(row[col]), table.columns):
        #         source_map[idx][col] = source_index_map[data_source.config["class"]]

    # Build a dataframe from the dictionary
    source_table = DataFrame(source_dict.values(), index=source_dict.keys())

    # Create a table with the source map
    index_adapter = {f"level_{idx}": col for idx, col in enumerate(index_columns)}
    source_table = source_table.reset_index().rename(columns=index_adapter)

    # Preserve the same order of the columns as the original table
    sorted_columns = [col for col in combined_table.columns if col in source_table.columns]
    source_table.columns = index_columns + sorted_columns

    return source_table


def create_metadata_dict() -> Dict[str, Any]:
    meta: Dict[str, Any] = {"tables": [], "sources": []}

    # Add each of the tables into the metadata file
    for pipeline in get_pipelines():
        fname = pipeline.table
        meta["tables"].append(
            {
                "name": fname,
                "label": pipeline.config.get("label"),
                "schema": pipeline.config.get("schema"),
                "description": pipeline.config.get("description"),
                "csv_url": f"https://storage.googleapis.com/covid19-open-data/v3/{fname}.csv",
                # TODO: discover the generation ID of the file and add it to the metadata
            }
        )

    # Add all the data sources to the metadata file
    sources = [(idx, pipeline, src) for idx, (pipeline, src) in enumerate(iter_data_sources())]
    meta["sources"] = [
        dict(src.config, index=idx, table=pipeline.table, uuid=src.uuid(pipeline.table))
        for idx, pipeline, src in sources
    ]

    return meta


def output_table_sources(source_table: DataFrame, output_path: str) -> None:

    # Create a schema to output integers and avoid pandas converting to floating point
    schema_map = dict(date="str", location_key="str")
    schema = {col: schema_map.get(col, "int") for col in source_table.columns}

    # Output the table at the requested location
    export_csv(source_table, output_path, schema)


if __name__ == "__main__":
    # To authenticate with Cloud locally, run the following commands:
    # > $env:GOOGLE_CLOUD_PROJECT = "github-open-covid-19"
    # > $env:GCS_SERVICE_ACCOUNT = "github-open-covid-19@appspot.gserviceaccount.com"
    # > $env:GCP_TOKEN = $(gcloud auth application-default print-access-token)

    # Create the output directory where the sources will go
    output_directory = SRC / ".." / "output" / "sources"
    output_directory.mkdir(exist_ok=True, parents=True)

    # Get the data sources and write a JSON file summarizing them to disk
    metadata = create_metadata_dict()
    with open(output_directory / "metadata.json", "w") as fh:
        json.dump(metadata, fh)

    # Iterate over the individual tables and build their sources file
    for table_name in ("epidemiology", "hospitalizations", "vaccinations", "by-age"):
        pipeline = DataPipeline.load(table_name.replace("-", "_"))
        source_map = map_table_sources_to_index(metadata["sources"], pipeline)
        output_table_sources(source_map, output_directory / f"{table_name}.sources.csv")
