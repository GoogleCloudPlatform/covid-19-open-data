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


import os
import re
import sys
import shutil
import tempfile
from pathlib import Path

# Add our library utils to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from lib.constants import SRC, URL_OUTPUTS_PROD
from lib.forecast import main as build_forecast
from lib.io import read_file, export_csv, pbar
from lib.net import download
from lib.utils import drop_na_records


def snake_to_camel_case(txt: str) -> str:
    """ Used to convert V2 column names to V1 column names for backwards compatibility """
    return re.sub(r"_(\w)", lambda m: m.group(1).upper(), txt.capitalize())


def main():

    # Create the folder which will be published
    public_folder = SRC / ".." / "output" / "public"
    public_folder.mkdir(exist_ok=True, parents=True)

    # Create the v1 data.csv file
    main_table = read_file(f"{URL_OUTPUTS_PROD}/main.csv", low_memory=False)
    data = main_table[main_table.aggregation_level < 2]
    rename_columns = {
        "date": "Date",
        "key": "Key",
        "country_code": "CountryCode",
        "country_name": "CountryName",
        "subregion1_code": "RegionCode",
        "subregion1_name": "RegionName",
        "total_confirmed": "Confirmed",
        "total_deceased": "Deaths",
        "latitude": "Latitude",
        "longitude": "Longitude",
        "population": "Population",
    }
    data = data[rename_columns.keys()].rename(columns=rename_columns)
    data = data.dropna(subset=["Confirmed", "Deaths"], how="all")
    data = data.sort_values(["Date", "Key"])
    export_csv(data, public_folder / "data.csv")

    # Create the v1 data_minimal.csv file
    export_csv(data[["Date", "Key", "Confirmed", "Deaths"]], public_folder / "data_minimal.csv")

    # Create the v1 data_latest.csv file
    latest = main_table[main_table.aggregation_level < 2]
    latest = latest.sort_values("date").groupby("key").last().reset_index()
    latest = latest[rename_columns.keys()].rename(columns=rename_columns)
    latest = latest.dropna(subset=["Confirmed", "Deaths"], how="all")
    latest = latest.sort_values(["Key", "Date"])
    export_csv(latest, public_folder / "data_latest.csv")

    # Create the v1 weather.csv file
    weather = read_file(f"{URL_OUTPUTS_PROD}/weather.csv")
    weather = weather[weather.key.apply(lambda x: len(x.split("_")) < 3)]
    weather = weather.rename(columns={"noaa_distance": "distance", "noaa_station": "station"})
    rename_columns = {col: snake_to_camel_case(col) for col in weather.columns}
    export_csv(weather.rename(columns=rename_columns), public_folder / "weather.csv")

    # Create the v1 mobility.csv file
    mobility = read_file(f"{URL_OUTPUTS_PROD}/mobility.csv")
    mobility = mobility[mobility.key.apply(lambda x: len(x.split("_")) < 3)]
    mobility = drop_na_records(mobility, ["date", "key"])
    rename_columns = {
        col: snake_to_camel_case(col).replace("Mobility", "") for col in mobility.columns
    }
    export_csv(mobility.rename(columns=rename_columns), public_folder / "mobility.csv")

    # Create the v1 CSV files which only require simple column mapping
    v1_v2_name_map = {"response": "oxford-government-response"}
    for v1_name, v2_name in v1_v2_name_map.items():
        data = read_file(f"{URL_OUTPUTS_PROD}/{v2_name}.csv")
        rename_columns = {col: snake_to_camel_case(col) for col in data.columns}
        export_csv(data.rename(columns=rename_columns), public_folder / f"{v1_name}.csv")

    # Create the v1 forecast.csv file
    export_csv(
        build_forecast(read_file(public_folder / "data_minimal.csv")),
        public_folder / "data_forecast.csv",
    )

    # Convert all v1 CSV files to JSON using record format
    for csv_file in pbar([*(public_folder).glob("*.csv")], desc="V1 JSON conversion"):
        data = read_file(csv_file, low_memory=False)
        json_path = str(csv_file).replace("csv", "json")
        data.to_json(json_path, orient="records")

    # Create the v2 folder
    v2_folder = public_folder / "v2"
    v2_folder.mkdir(exist_ok=True, parents=True)

    # Download the v2 tables which can fit under 100MB
    for table_name in pbar(
        (
            "by-age",
            "by-sex",
            "demographics",
            "economy",
            "epidemiology",
            "geography",
            "health",
            "hospitalizations",
            "index",
            "mobility",
            "oxford-government-response",
            "weather",
            "worldbank",
            "worldpop",
        ),
        desc="V2 download",
    ):
        for ext in ("csv", "json"):
            with tempfile.NamedTemporaryFile() as tmp:
                tmp_path = Path(tmp.name)
                download(f"{URL_OUTPUTS_PROD}/{table_name}.{ext}", tmp)
                # Check that the output is less than 100 MB before copying it to the output folder
                if tmp_path.stat().st_size < 100 * 1000 * 1000:
                    shutil.copyfile(tmp_path, v2_folder / f"{table_name}.{ext}")


if __name__ == "__main__":
    main()
