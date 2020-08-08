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
from random import shuffle
from functools import partial
from typing import Dict

import numpy
from pandas import DataFrame, Series, concat

from lib.cast import safe_float_cast
from lib.concurrent import thread_map
from lib.data_source import DataSource
from lib.io import pbar, read_file


_COLUMN_MAPPING = {
    "DATE": "date",
    "STATION": "noaa_station",
    "TEMP": "average_temperature",
    "MIN": "minimum_temperature",
    "MAX": "maximum_temperature",
    "PRCP": "rainfall",
    "SNDP": "snowfall",
    "DEWP": "dew_point",
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
    "dew_point",
]
_DISTANCE_THRESHOLD = 300


def haversine_distance(
    stations: DataFrame, lat: float, lon: float, radius: float = 6373.0
) -> Series:
    """ Compute the distance between two <latitude, longitude> pairs in kilometers """

    # Compute the pairwise deltas
    lat_diff = stations["lat"] - lat
    lon_diff = stations["lon"] - lon

    # Apply Haversine formula
    a = numpy.sin(lat_diff / 2) ** 2
    a += math.cos(lat) * numpy.cos(stations["lat"]) * numpy.sin(lon_diff / 2) ** 2
    c = numpy.arctan2(numpy.sqrt(a), numpy.sqrt(1 - a)) * 2

    return radius * c


def noaa_number(value: int):
    return None if re.match(r"999+", str(value).replace(".", "")) else safe_float_cast(value)


def conv_temp(value: int):
    value = noaa_number(value)
    return numpy.nan if value is None else (value - 32) * 5 / 9


def conv_dist(value: int):
    value = noaa_number(value)
    return numpy.nan if value is None else value * 25.4


def relative_humidity(temp: float, dew_point: float) -> float:
    """ http://bmcnoldy.rsmas.miami.edu/humidity_conversions.pdf """
    a = 17.625
    b = 243.04
    return 100 * numpy.exp(a * dew_point / (b + dew_point)) / numpy.exp(a * temp / (b + temp))


class NoaaGsodDataSource(DataSource):
    @staticmethod
    def process_location(
        station_cache: Dict[str, DataFrame], stations: DataFrame, location: Series
    ):
        # Get the nearest stations from our list of stations given lat and lon
        distance = haversine_distance(stations, location.lat, location.lon)

        # Filter out all but the 10 nearest stations
        mask = distance < _DISTANCE_THRESHOLD
        nearest = stations.loc[mask].copy()
        nearest["key"] = location.key
        nearest["distance"] = distance.loc[mask]
        nearest.sort_values("distance", inplace=True)
        nearest = nearest.iloc[:10]

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
            "dew_point",
        ]
        agg_functions = {col: "mean" for col in value_columns}
        agg_functions["noaa_station"] = "first"
        agg_functions["noaa_distance"] = "first"
        data = data.groupby(["date", "key"]).agg(agg_functions).reset_index()

        # Return all the available data from the records
        return data[[col for col in _OUTPUT_COLUMNS if col in data.columns]]

    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        # Get all the weather stations with data up until last month from inventory
        today = datetime.date.today()
        min_date = (today - datetime.timedelta(days=30)).strftime("%Y%m%d")
        stations = read_file(sources["inventory"]).rename(
            columns={"LAT": "lat", "LON": "lon", "ELEV(M)": "elevation"}
        )
        stations = stations[stations.END > int(min_date)]
        stations["id"] = stations["USAF"] + stations["WBAN"].apply(lambda x: f"{x:05d}")

        # Open the station data as a compressed file
        with tarfile.open(sources["gsod"], mode="r:gz") as stations_tar:

            # Build the station cache by uncompressing all files in memory
            station_cache = {}
            for member in pbar(stations_tar.getmembers(), desc="Decompressing"):

                if not member.name.endswith(".csv"):
                    continue

                # Read the records from the provided station
                data = read_file(
                    stations_tar.extractfile(member),
                    file_type="csv",
                    usecols=_COLUMN_MAPPING.keys(),
                ).rename(columns=_COLUMN_MAPPING)

                # Fix data types
                data["noaa_station"] = data["noaa_station"].astype(str)
                data["rainfall"] = data["rainfall"].apply(conv_dist)
                data["snowfall"] = data["snowfall"].apply(conv_dist)
                data["dew_point"] = data["dew_point"].apply(conv_temp)
                for temp_type in ("average", "minimum", "maximum"):
                    col = f"{temp_type}_temperature"
                    data[col] = data[col].apply(conv_temp)

                # Compute the relative humidity from the dew point and average temperature
                data["relative_humidity"] = data.apply(
                    lambda x: relative_humidity(x["average_temperature"], x["dew_point"]), axis=1
                )

                station_cache[member.name.replace(".csv", "")] = data

        # Get all the POI from metadata and go through each key
        keep_columns = ["key", "latitude", "longitude"]
        metadata = read_file(sources["geography"])[keep_columns].dropna()

        # Only use keys present in the metadata table
        metadata = metadata.merge(aux["metadata"])[keep_columns]

        # Convert all coordinates to radians
        stations["lat"] = stations["lat"].apply(math.radians)
        stations["lon"] = stations["lon"].apply(math.radians)
        metadata["lat"] = metadata["latitude"].apply(math.radians)
        metadata["lon"] = metadata["longitude"].apply(math.radians)

        # Make sure the stations and the cache are sent to each function call
        map_func = partial(NoaaGsodDataSource.process_location, station_cache, stations)

        # We don't care about the index while iterating over each metadata item
        map_iter = [record for _, record in metadata.iterrows()]

        # Shuffle the iterables to try to make better use of the caching
        shuffle(map_iter)

        # Bottleneck is network so we can use lots of threads in parallel
        records = thread_map(map_func, map_iter, total=len(metadata), max_workers=4)

        return concat(records)
