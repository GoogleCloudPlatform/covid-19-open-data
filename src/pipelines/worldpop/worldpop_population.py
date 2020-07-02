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

import zipfile
from io import BytesIO
from functools import partial
from typing import Any, Dict, List, Tuple

import requests
from tqdm import tqdm
from pandas import DataFrame, Series, read_csv, isnull

from lib.concurrent import thread_map
from lib.net import download
from lib.pipeline import DataSource
from lib.cast import age_group


def _aggregate_population(data: Series) -> Dict[str, int]:
    """
    WorldPop data is "double stacked" by breaking down by age *and* sex, whereas we only want
    a breakdown of age *or* sex. This function converts the columns:
    `[
        m_0,
        m_5,
        ...
        f_0,
        f_5,
        ...
    ]`

    Into:
    `[
        population,
        population_male,
        population_female,
        population_age_00_09,
        population_age_10_19,
        ...
    ]`
    """
    age_bucket_size = 10
    age_bucket_count = 10
    age_bucket_pairs = [
        (i * age_bucket_size, (i + 1) * age_bucket_size - 1) for i in range(age_bucket_count)
    ]

    aggregated_values = {
        "key": data["key"],
        "population": 0,
        "population_male": 0,
        "population_female": 0,
        **{f"population_age_{lo:02d}_{hi:02d}": 0 for lo, hi in age_bucket_pairs},
    }
    for col, val in data.iteritems():
        # Skip over the key item
        if col == "key":
            continue

        # Total population is the sum of all populations
        aggregated_values["population"] += val

        # Get age/sex info from column name
        sex, age = col.split("_", 2)

        # Add male and female populations separately
        if sex == "m":
            aggregated_values["population_male"] += val
        elif sex == "f":
            aggregated_values["population_female"] += val
        else:
            raise ValueError(f"Unexpected sex label encountered: {sex}")

        # Go over all the age buckets and add them separately
        # Since the WorldPop buckets are [0, 1-5, 5-9, 10-14, ...] we can just use the lower
        # range of the bucket as the age value to convert into our normalized buckets which are
        # [0-9, 10-19, 20-29, ...]
        age_bucket = age_group(int(age))

        # Make sure that the age buckets all follow the pattern \d\d_\d\d
        age_bucket = "_".join([f"{int(age):02d}" for age in age_bucket.split("-", 2)])

        aggregated_values[f"population_age_{age_bucket}"] += val

    return aggregated_values


class WorldPopPopulationDataSource(DataSource):
    """
    Retrieves demographics information from WorldPop for all items in metadata.csv. The original
    data source is https://www.worldpop.org/project/categories?id=8 but the data is pre-processed
    using Google Earth Engine: https://code.earthengine.google.com/f885dd559364ed8918324da355c1ee0e
    """

    def parse(self, sources: List[str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        # In this script, we don't have any sources since the Worldpop data is precomputed using
        # Earth Engine: https://code.earthengine.google.com/f885dd559364ed8918324da355c1ee0e

        # Compute the estimated aggregated population broken down by age and sex
        records = aux["worldpop"].apply(_aggregate_population, axis=1)
        data = DataFrame.from_records(records.values).merge(aux["worldpop"])

        # Keep only records which are part of the index
        data = data.merge(aux["metadata"][["key"]])

        # WorldPop only provides data for people up to 80 years old, but we want 10-year buckets
        # until 90, and 90+ instead. We estimate that, within the 80+ group, 80% are 80-90 and
        # 20% are 90+. This is based on multiple reports, for example:
        # https://www.statista.com/statistics/281174/uk-population-by-age
        data["population_age_90_99"] = data["population_age_80_89"] * 0.2
        data["population_age_80_89"] = data["population_age_80_89"] * 0.8

        return data
