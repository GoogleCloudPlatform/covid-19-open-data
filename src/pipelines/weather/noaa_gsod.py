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

import re
import math
import tarfile
import datetime
from io import BytesIO
from random import shuffle
from pathlib import Path
from functools import partial
from typing import Any, Dict, List

import numpy
from tqdm import tqdm
from tqdm.contrib import concurrent
from pandas import DataFrame, Series, read_csv, concat

from lib.cast import safe_float_cast
from lib.net import download
from lib.pipeline import DataSource
from lib.utils import ROOT


_COLUMN_MAPPING = {
    "DATE": "date",
    "STATION": "noaa_station",
    "TEMP": "average_temperature",
    "MIN": "minimum_temperature",
    "MAX": "maximum_temperature",
    "PRCP": "rainfall",
    "SNDP": "snowfall",
}
_OUTPUT_COLUMNS = [
    "date",
    "key",
    "noaa_station",
    "noaa_distance",
    "average_temperature",
    "minimum_temperature",
    "maximum_temperature",
    "rainfall",
    "snowfall",
]
_DISTANCE_THRESHOLD = 300
_INVENTORY_URL = "https://www1.ncdc.noaa.gov/pub/data/noaa/isd-history.csv"


class NoaaGsodDataSource(DataSource):

    # A bit of a circular dependency but we need the latitude and longitude to compute weather
    def fetch(
        self, output_folder: Path, cache: Dict[str, str], fetch_opts: List[Dict[str, Any]]
    ) -> List[str]:
        return [ROOT / "output" / "tables" / "geography.csv"]

    @staticmethod
    def haversine_distance(
        stations: DataFrame, lat: float, lon: float, radius: float = 6373.0
    ) -> Series:
        """ Compute the distance between two <latitude, longitude> pairs in kilometers """

        # Compute the pairwise deltas
        lat_diff = stations.lat - lat
        lon_diff = stations.lon - lon

        # Apply Haversine formula
        a = numpy.sin(lat_diff / 2) ** 2
        a += math.cos(lat) * numpy.cos(stations.lat) * numpy.sin(lon_diff / 2) ** 2
        c = numpy.arctan2(numpy.sqrt(a), numpy.sqrt(1 - a)) * 2

        return radius * c

    @staticmethod
    def noaa_number(value: int):
        return None if re.match(r"999+", str(value).replace(".", "")) else safe_float_cast(value)

    @staticmethod
    def conv_temp(value: int):
        value = NoaaGsodDataSource.noaa_number(value)
        return numpy.nan if value is None else (value - 32) * 5 / 9

    @staticmethod
    def conv_dist(value: int):
        value = NoaaGsodDataSource.noaa_number(value)
        return numpy.nan if value is None else value * 25.4

    @staticmethod
    def process_location(
        station_cache: Dict[str, DataFrame], stations: DataFrame, location: Series
    ):
        nearest = stations.copy()
        nearest["key"] = location.key

        # Get the nearest stations from our list of stations given lat and lon
        nearest["distance"] = NoaaGsodDataSource.haversine_distance(
            nearest, location.lat, location.lon
        )

        # Filter out all but the 10 nearest stations
        nearest = nearest[nearest.distance < _DISTANCE_THRESHOLD].sort_values("distance").iloc[:10]

        # Early exit: no stations found within distance threshold
        if len(nearest) == 0 or all(
            station_id not in station_cache for station_id in nearest.id.values
        ):
            return DataFrame(columns=_OUTPUT_COLUMNS)

        # Get station records from the cache
        nearest = nearest.rename(columns={"id": "noaa_station", "distance": "noaa_distance"})
        data = [station_cache.get(station_id) for station_id in nearest.noaa_station.values]
        data = concat(
            [table.merge(nearest, on="noaa_station") for table in data if table is not None]
        )

        # Combine them by computing a simple average
        value_columns = [
            "average_temperature",
            "minimum_temperature",
            "maximum_temperature",
            "rainfall",
            "snowfall",
        ]
        agg_functions = {col: "mean" for col in value_columns}
        agg_functions["noaa_station"] = "first"
        agg_functions["noaa_distance"] = "first"
        data = data.groupby(["date", "key"]).agg(agg_functions).reset_index()

        # Return all the available data from the records
        return data[[col for col in _OUTPUT_COLUMNS if col in data.columns]]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ):

        # Get all the weather stations with data up until last month from inventory
        today = datetime.date.today()
        min_date = (today - datetime.timedelta(days=30)).strftime("%Y%m%d")
        stations = read_csv(_INVENTORY_URL).rename(
            columns={"LAT": "lat", "LON": "lon", "ELEV(M)": "elevation"}
        )
        stations = stations[stations.END > int(min_date)]
        stations["id"] = stations["USAF"] + stations["WBAN"].apply(lambda x: f"{x:05d}")

        # Download all the station data as a compressed file
        buffer = BytesIO()
        records_url = "https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive/2020.tar.gz"
        download(records_url, buffer, progress=True)
        buffer.seek(0)
        with tarfile.open(fileobj=buffer, mode="r:gz") as stations_tar:

            # Build the station cache by uncompressing all files in memory
            station_cache = {}
            for member in tqdm(stations_tar.getmembers(), desc="Decompressing"):

                if not member.name.endswith(".csv"):
                    continue

                # Read the records from the provided station
                data = read_csv(stations_tar.extractfile(member)).rename(columns=_COLUMN_MAPPING)

                # Fix data types
                data.noaa_station = data.noaa_station.astype(str)
                data.rainfall = data.rainfall.apply(NoaaGsodDataSource.conv_dist)
                data.snowfall = data.snowfall.apply(NoaaGsodDataSource.conv_dist)
                for temp_type in ("average", "minimum", "maximum"):
                    col = f"{temp_type}_temperature"
                    data[col] = data[col].apply(NoaaGsodDataSource.conv_temp)

                station_cache[member.name.replace(".csv", "")] = data

        # Get all the POI from metadata and go through each key
        metadata = dataframes[0][["key", "latitude", "longitude"]].dropna()

        # Convert all coordinates to radians
        stations["lat"] = stations.lat.apply(math.radians)
        stations["lon"] = stations.lon.apply(math.radians)
        metadata["lat"] = metadata.latitude.apply(math.radians)
        metadata["lon"] = metadata.longitude.apply(math.radians)

        # Make sure the stations and the cache are sent to each function call
        map_func = partial(NoaaGsodDataSource.process_location, station_cache, stations)

        # We don't care about the index while iterating over each metadata item
        map_iter = [record for _, record in metadata.iterrows()]

        # Shuffle the iterables to try to make better use of the caching
        shuffle(map_iter)

        # Bottleneck is network so we can use lots of threads in parallel
        records = concurrent.thread_map(map_func, map_iter, total=len(metadata))

        return concat(records)
