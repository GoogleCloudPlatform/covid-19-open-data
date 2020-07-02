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

import math
from random import shuffle
from pathlib import Path
from functools import partial
from typing import Any, Dict, List

import numpy
from tqdm.contrib import concurrent
from pandas import DataFrame, Series, read_csv, concat

from lib.cast import safe_int_cast
from lib.pipeline import DataSource
from lib.utils import ROOT, combine_tables


_COLUMN_MAPPING = {
    "DATE": "date",
    "STATION": "noaa_station",
    "TMIN": "minimum_temperature",
    "TMAX": "maximum_temperature",
    "PRCP": "rainfall",
    "SNOW": "snowfall",
}
_OUTPUT_COLUMNS = [
    "date",
    "key",
    "noaa_station",
    "noaa_distance",
    "minimum_temperature",
    "maximum_temperature",
    "rainfall",
    "snowfall",
]
_DISTANCE_THRESHOLD = 300
_INVENTORY_URL = "https://open-covid-19.github.io/weather/ghcn/ghcnd-inventory.txt"
_STATION_URL_TPL = "https://open-covid-19.github.io/weather/ghcn/{}.csv"
# _INVENTORY_URL = "https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-inventory.txt"
# _STATION_URL_TPL = (
#     "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/access/{}.csv"
# )


class NoaaGhcnDataSource(DataSource):

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
    def fix_temp(value: int):
        value = safe_int_cast(value)
        return None if value is None else "%.1f" % (value / 10.0)

    @staticmethod
    def station_records(station_cache: Dict[str, DataFrame], stations: DataFrame, location: Series):
        nearest = stations.copy()
        nearest["key"] = location.key

        # Get the nearest stations from our list of stations given lat and lon
        nearest["distance"] = NoaaGhcnDataSource.haversine_distance(
            nearest, location.lat, location.lon
        )

        # Filter out the 10 nearest stations
        nearest = nearest[nearest.distance < _DISTANCE_THRESHOLD].sort_values("distance").iloc[:20]

        # Early exit: no stations found within distance threshold
        if len(nearest) == 0:
            return DataFrame(columns=_OUTPUT_COLUMNS)

        # Query the cache and pull data only if not already cached
        for station_id in filter(lambda x: x not in station_cache, nearest.id.values):

            # Read the records from the nearest station
            # Use our mirror since NOAA's website is very flaky
            station_url = _STATION_URL_TPL.format(station_id)
            data = read_csv(station_url, usecols=lambda column: column in _COLUMN_MAPPING.keys())
            data = data.rename(columns=_COLUMN_MAPPING)

            # Convert temperature to correct values
            data["minimum_temperature"] = data["minimum_temperature"].apply(
                NoaaGhcnDataSource.fix_temp
            )
            data["maximum_temperature"] = data["maximum_temperature"].apply(
                NoaaGhcnDataSource.fix_temp
            )

            # Get only data for 2020 and add location values
            data = data[data.date > "2019-12-31"]

            # Save into the cache
            station_cache[station_id] = data

        # Get station records from the cache
        nearest = nearest.rename(columns={"id": "noaa_station", "distance": "noaa_distance"})
        station_tables = [station_cache[station_id] for station_id in nearest.noaa_station.values]
        station_tables = [table.merge(nearest) for table in station_tables]
        data = combine_tables(reversed(station_tables), ["date", "key"])

        # Return all the available data from the records
        return data[[col for col in _OUTPUT_COLUMNS if col in data.columns]]

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ):

        # Get all the weather stations with data up until 2020
        stations = read_csv(
            _INVENTORY_URL,
            sep=r"\s+",
            names=("id", "lat", "lon", "measurement", "year_start", "year_end"),
        )
        stations = stations[stations.year_end == 2020][["id", "lat", "lon", "measurement"]]

        # Filter stations that at least provide max and min temps
        measurements = ["TMIN", "TMAX"]
        stations = stations.groupby(["id", "lat", "lon"]).agg(lambda x: "|".join(x))
        stations = stations[stations.measurement.apply(lambda x: all(m in x for m in measurements))]
        stations = stations.reset_index()

        # Get all the POI from metadata and go through each key
        metadata = dataframes[0][["key", "latitude", "longitude"]].dropna()

        # Convert all coordinates to radians
        stations["lat"] = stations.lat.apply(math.radians)
        stations["lon"] = stations.lon.apply(math.radians)
        metadata["lat"] = metadata.latitude.apply(math.radians)
        metadata["lon"] = metadata.longitude.apply(math.radians)

        # Use a cache to avoid having to query the same station multiple times
        station_cache: Dict[str, DataFrame] = {}

        # Make sure the stations and the cache are sent to each function call
        map_func = partial(NoaaGhcnDataSource.station_records, station_cache, stations)

        # We don't care about the index while iterating over each metadata item
        map_iter = [record for _, record in metadata.iterrows()]

        # Shuffle the iterables to try to make better use of the caching
        shuffle(map_iter)

        # Bottleneck is network so we can use lots of threads in parallel
        records = concurrent.thread_map(map_func, map_iter, total=len(metadata))

        return concat(records)
