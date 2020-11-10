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

import cProfile
import re
from pstats import Stats
from pathlib import Path
from argparse import ArgumentParser
from multiprocessing import cpu_count
from typing import List

from lib.constants import SRC
from lib.io import export_csv
from lib.pipeline import DataPipeline


def main(
    output_folder: Path,
    verify: str = None,
    only: List[str] = None,
    exclude: List[str] = None,
    location_key: str = None,
    strict_match: bool = False,
    process_count: int = cpu_count(),
    skip_download: bool = False,
) -> None:
    """
    Executes the data pipelines and places all outputs into `output_folder`. This is typically
    followed by publishing of the contents of the output folder to a server.

    Args:
        output_folder: Root folder where snapshot, intermediate and tables will be placed.
        verify: Run anomaly detection on the outputs using this strategy. Value must be one of:
            - None: (default) perform no anomaly detection
            - "simple": perform only fast anomaly detection
            - "full": perform exhaustive anomaly detection (can be very slow)
        only: If provided, only pipelines with a name appearing in this list will be run.
        exclude: If provided, pipelines with a name appearing in this list will not be run.
        location_key: If present, only run data sources which output data for this location.
        strict_match: In combination with `location_key`, filter data to only output `location_key`.
        process_count: Maximum number of processes to use during the data pipeline execution.
        skip_download: Skip downloading data sources if a cached version is available.
    """

    assert not (
        only is not None and exclude is not None
    ), "--only and --exclude options cannot be used simultaneously"

    # Ensure that there is an output folder to put the data in
    (output_folder / "snapshot").mkdir(parents=True, exist_ok=True)
    (output_folder / "intermediate").mkdir(parents=True, exist_ok=True)
    (output_folder / "tables").mkdir(parents=True, exist_ok=True)

    # A pipeline chain is any subfolder not starting with "_" in the pipelines folder
    all_pipeline_names = []
    for item in (SRC / "pipelines").iterdir():
        if not item.name.startswith("_") and not item.is_file():
            all_pipeline_names.append(item.name)

    # Verify that all of the provided pipeline names exist as pipelines
    for pipeline_name in (only or []) + (exclude or []):
        module_name = pipeline_name.replace("-", "_")
        assert module_name in all_pipeline_names, f'"{pipeline_name}" pipeline does not exist'

    # Run all the pipelines and place their outputs into the output folder. The output name for
    # each pipeline chain will be the name of the directory that the chain is in.
    for pipeline_name in all_pipeline_names:
        table_name = pipeline_name.replace("_", "-")

        # Skip if `exclude` was provided and this table is in it
        if exclude is not None and table_name in exclude:
            continue

        # Skip is `only` was provided and this table is not in it
        if only is not None and not table_name in only:
            continue

        # Load data pipeline and get rid of data sources if requested
        data_pipeline = DataPipeline.load(pipeline_name)
        if location_key is not None:
            exprs = [
                src.config.get("test", {}).get("location_key_match", ".*")
                for src in data_pipeline.data_sources
            ]
            exprs = [expr if isinstance(expr, list) else [expr] for expr in exprs]
            data_pipeline.data_sources = [
                src
                for src, expr in zip(data_pipeline.data_sources, exprs)
                if any(re.match(expr_, location_key) for expr_ in expr)
            ]

        # Run the data pipeline to retrieve live data
        pipeline_output = data_pipeline.run(
            output_folder,
            process_count=process_count,
            verify_level=verify,
            skip_existing=skip_download,
        )

        # Filter out data output if requested
        if location_key is not None and strict_match:
            pipeline_output = pipeline_output[pipeline_output["key"] == location_key]

        # Export the data output to disk as a CSV file
        export_csv(
            pipeline_output,
            output_folder / "tables" / f"{table_name}.csv",
            schema=data_pipeline.schema,
        )


if __name__ == "__main__":

    # Process command-line arguments
    argparser = ArgumentParser()
    argparser.add_argument("--only", type=str, default=None)
    argparser.add_argument("--exclude", type=str, default=None)
    argparser.add_argument("--location-key", type=str, default=None)
    argparser.add_argument("--strict-match", action="store_true")
    argparser.add_argument("--skip-download", action="store_true")
    argparser.add_argument("--verify", type=str, default=None)
    argparser.add_argument("--profile", action="store_true")
    argparser.add_argument("--process-count", type=int, default=cpu_count())
    argparser.add_argument("--output-folder", type=str, default=str(SRC / ".." / "output"))
    args = argparser.parse_args()

    if args.profile:
        profiler = cProfile.Profile()
        profiler.enable()

    only = args.only.split(",") if args.only is not None else None
    exclude = args.exclude.split(",") if args.exclude is not None else None

    main(
        Path(args.output_folder),
        verify=args.verify,
        only=only,
        exclude=exclude,
        location_key=args.location_key,
        strict_match=args.strict_match,
        process_count=args.process_count,
        skip_download=args.skip_download,
    )

    if args.profile:
        stats = Stats(profiler)
        stats.strip_dirs()
        stats.sort_stats("cumtime")
        stats.print_stats(20)
