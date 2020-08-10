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

"""
Script used to download geometries from all regions which have an OSM relation
ID associated with them. It makes use of the openstreetmap API and requires
local installation of the ogr2ogr binary.
"""

from argparse import ArgumentParser
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Any, Dict, List

from pandas import DataFrame
import requests
from tqdm import tqdm

# Add our library utils to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# pylint: disable=wrong-import-position
from lib.constants import SRC
from lib.io import read_file

GEO_URL = "https://storage.googleapis.com/covid19-open-data/v2/geography.csv"
OSM_API = "http://polygons.openstreetmap.fr/get_geojson.py?id={id}&params=0"


def fetch_geometry() -> DataFrame:
    data = read_file(GEO_URL).dropna(subset=["open_street_maps"])
    data["open_street_maps"] = data["open_street_maps"].astype(int).astype(str)
    return data


def fetch_geojson(geojson_directory: Path, osm_records: List[Dict[str, Any]]) -> bytes:
    geojson_directory.mkdir(exist_ok=True, parents=True)
    for key, record in tqdm(osm_records, desc="Downloading GeoJSON files"):
        geojson_path = geojson_directory / f"{key}.geojson"
        # Skip the download if the file already exists
        if geojson_path.exists():
            continue
        relation_id = record["open_street_maps"]
        geojson_data = requests.get(OSM_API.format(id=relation_id)).content
        if geojson_data and geojson_data.decode("utf8").strip() != "None":
            with open(geojson_path, "wb") as fd:
                fd.write(geojson_data)


def convert_geojson_to_csv(geojson_directory: Path, csv_path: Path) -> None:
    csv_path.parent.mkdir(exist_ok=True, parents=True)
    with tempfile.TemporaryDirectory() as workdir:
        workdir = Path(workdir)

        geojson_files = list(sorted(geojson_directory.glob("*.geojson")))
        for geojson_path in tqdm(geojson_files, desc="Converting GeoJSON to CSV"):
            csv_tmp = workdir / geojson_path.name.replace(".geojson", ".csv")
            ogr_args = ["-f", "CSV", "-lco", "GEOMETRY=AS_WKT", csv_tmp, geojson_path]
            subprocess.check_call(["ogr2ogr"] + ogr_args)

        with open(csv_path, "w") as fd_csv:
            fd_csv.write("key,WKT\n")
            csv_files = list(sorted(workdir.glob("*.csv")))
            for csv_path in tqdm(csv_files, desc="Merging all CSV files"):
                with open(csv_tmp, "r") as fd_tmp:
                    fd_csv.write(f"{csv_path.stem},{fd_tmp.readlines()[-1]}")


if __name__ == "__main__":

    # Process command-line arguments
    output_root = SRC / ".." / "output"
    argparser = ArgumentParser()
    argparser.add_argument("--output-folder", type=str, default=str(output_root / "geometries"))
    args = argparser.parse_args()

    output_dir = Path(args.output_folder)
    geojson_dir = output_dir / "geojson"
    csv_dir = output_dir / "csv"

    geo = fetch_geometry().set_index("key")
    fetch_geojson(geojson_dir, list(geo.iterrows()))
    convert_geojson_to_csv(output_dir, csv_dir / "open-covid-admin.csv")
