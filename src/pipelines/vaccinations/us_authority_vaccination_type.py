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
from functools import partial
from lib.concurrent import process_map
from typing import Dict
from pandas import DataFrame, concat
from lib.cached_data_source import CachedDataSource
from lib.utils import table_rename
from lib.io import read_file


_column_adapter = {
    'Series_Complete_Yes': 'total_persons_fully_vaccinated',
    'Doses_Administered': 'total_vaccine_doses_administered',
    'Administered_Dose1_Recip':'total_persons_vaccinated',

    'Series_Complete_Pfizer': 'total_persons_fully_vaccinated_pfizer',
    'Administered_Pfizer': 'total_vaccine_doses_administered_pfizer',

    'Series_Complete_Moderna': 'total_persons_fully_vaccinated_moderna',
    'Administered_Moderna': 'total_vaccine_doses_administered_moderna',

    'Series_Complete_Janssen': 'total_persons_fully_vaccinated_janssen',
    'Administered_Janssen': 'total_vaccine_doses_administered_janssen',

    "key": "key"
    }

states = [
    'US',
    'AK',
    'AL',
    'AR',
    'AS',
    'AZ',
    'CA',
    'CO',
    'CT',
    'DC',
    'DE',
    'FL',
    'GA',
    'GU',
    'HI',
    'IA',
    'ID',
    'IL',
    'IN',
    'KS',
    'KY',
    'LA',
    'MA',
    'MD',
    'ME',
    'MI',
    'MN',
    'MO',
    'MP',
    'MS',
    'MT',
    'NC',
    'ND',
    'NE',
    'NH',
    'NJ',
    'NM',
    'NV',
    'NY',
    'OH',
    'OK',
    'OR',
    'PA',
    'PR',
    'RI',
    'SC',
    'SD',
    'TN',
    'TX',
    'UT',
    'VA',
    'VI',
    'VT',
    'WA',
    'WI',
    'WV',
    'WY'
 ]


def _process_cache_file(file_map: Dict[str, str], date: str) -> DataFrame:
    data = read_file(file_map[date])["vaccination_data"].values.tolist()
    data = DataFrame([list(v.values()) for v in data], columns=list(data[0].keys()))

    data = data.loc[data.Location.isin(states)]
    for col in set(_column_adapter.keys()).intersection(data.columns):
        data[col] = data[col].fillna(0).astype(int)

    data["key"] = data["Location"].apply(lambda x: "US" if x=="US" else "US_" + x[:2])

    data = table_rename(data, _column_adapter, drop=True)

    data["date"] = date

    return data


class CDCCachedDataSourceVaccinationType(CachedDataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        file_map = sources["US_CDC_states_vaccinations"]

        map_func = partial(_process_cache_file, file_map)
        map_opts = dict(desc="Processing Cache Files", total=len(file_map))
        data = concat(process_map(map_func, file_map.keys(), **map_opts))

        assert len(data) > 0, "No records were found"
        return data
