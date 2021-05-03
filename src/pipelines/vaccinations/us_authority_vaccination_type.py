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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> aeb73b3c8 (Vaccination by type:)
from pandas import DataFrame, concat, read_csv
from lib.cached_data_source import CachedDataSource
from lib.utils import table_rename
from lib.io import read_file
from lib.constants import SRC
<<<<<<< HEAD
<<<<<<< HEAD
=======
from pandas import DataFrame, concat
=======
from pandas import DataFrame, concat, read_csv
>>>>>>> 9238b563f (added us_cdc_locations for reuse)
=======
=======
from pandas import DataFrame, concat
>>>>>>> aeb73b3c8 (Vaccination by type:)
from lib.cached_data_source import CachedDataSource
from lib.utils import table_rename
from lib.io import read_file
>>>>>>> e2f8dcce6 (Vaccination by type:)
<<<<<<< HEAD
=======
>>>>>>> ff4effaf5 (Added SRC to reader)
=======
>>>>>>> aeb73b3c8 (Vaccination by type:)


_column_adapter = {
    'Series_Complete_Yes': 'total_persons_fully_vaccinated',
    'Doses_Administered': 'total_vaccine_doses_administered',
    'Administered_Dose1_Recip':'total_persons_vaccinated',

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> aeb73b3c8 (Vaccination by type:)
    'Series_Complete_Pfizer': 'total_persons_fully_vaccinated_pfizer',
    'Administered_Pfizer': 'total_vaccine_doses_administered_pfizer',

    'Series_Complete_Moderna': 'total_persons_fully_vaccinated_moderna',
    'Administered_Moderna': 'total_vaccine_doses_administered_moderna',

    'Series_Complete_Janssen': 'total_persons_fully_vaccinated_janssen',
    'Administered_Janssen': 'total_vaccine_doses_administered_janssen',
=======
    'Series_Complete_Pfizer': 'total_persons_fully_vaccinated_Pfizer',
    'Administered_Pfizer': 'total_vaccine_doses_administered_Pfizer',
<<<<<<< HEAD
=======
    'Series_Complete_Pfizer': 'total_persons_fully_vaccinated_pfizer',
    'Administered_Pfizer': 'total_vaccine_doses_administered_pfizer',
>>>>>>> 6d37c6e88 (Vaccination by type)

    'Series_Complete_Moderna': 'total_persons_fully_vaccinated_moderna',
    'Administered_Moderna': 'total_vaccine_doses_administered_moderna',

<<<<<<< HEAD
    'Series_Complete_Janssen': 'total_persons_fully_vaccinated_Janssen',
    'Administered_Janssen': 'total_vaccine_doses_administered_Janssen',
>>>>>>> e2f8dcce6 (Vaccination by type:)
=======
    'Series_Complete_Janssen': 'total_persons_fully_vaccinated_janssen',
    'Administered_Janssen': 'total_vaccine_doses_administered_janssen',
>>>>>>> 6d37c6e88 (Vaccination by type)
=======

    'Series_Complete_Moderna': 'total_persons_fully_vaccinated_Moderna',
    'Administered_Moderna': 'total_vaccine_doses_administered_Moderna',

    'Series_Complete_Janssen': 'total_persons_fully_vaccinated_Janssen',
    'Administered_Janssen': 'total_vaccine_doses_administered_Janssen',
>>>>>>> e2f8dcce6 (Vaccination by type:)
>>>>>>> aeb73b3c8 (Vaccination by type:)

    "key": "key"
    }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
us_states = ['US'] + read_csv(SRC / "data" / "us_cdc_locations.csv").key.values.tolist()
=======
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

>>>>>>> e2f8dcce6 (Vaccination by type:)
=======
us_states = ['US'] + read_csv("data/us_cdc_locations.csv").key.values.tolist()
>>>>>>> 9238b563f (added us_cdc_locations for reuse)
=======
us_states = ['US'] + read_csv(SRC / "data" / "us_cdc_locations.csv").key.values.tolist()
>>>>>>> ff4effaf5 (Added SRC to reader)
=======
us_states = ['US'] + read_csv(SRC / "data" / "us_cdc_locations.csv").key.values.tolist()
=======
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
 'WY']

>>>>>>> e2f8dcce6 (Vaccination by type:)
>>>>>>> aeb73b3c8 (Vaccination by type:)

def _process_cache_file(file_map: Dict[str, str], date: str) -> DataFrame:
    data = read_file(file_map[date])["vaccination_data"].values.tolist()
    data = DataFrame([list(v.values()) for v in data], columns=list(data[0].keys()))

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
    data = data.loc[data.Location.isin(us_states)]
    for col in set(_column_adapter.keys()).intersection(data.columns):
        data[col] = data[col].fillna(0).astype(int)

    data["key"] = data["Location"].apply(lambda x: "US" if x=="US" else "US_" + x[:2])

=======
    data = data.loc[data.Location.isin(states)]
=======
=======
>>>>>>> aeb73b3c8 (Vaccination by type:)
    data = data.loc[data.Location.isin(us_states)]
>>>>>>> 9238b563f (added us_cdc_locations for reuse)
    for col in set(_column_adapter.keys()).intersection(data.columns):
        data[col] = data[col].fillna(0).astype(int)

    data["key"] = data["Location"].apply(lambda x: "US" if x=="US" else "US_" + x[:2])

<<<<<<< HEAD
=======
=======
    data = data.loc[data.Location.isin(states)]
for col in set(_column_adapter.keys()).intersect(data.columns):
    data[col] = data[col].fillna(0).astype(int)

data["key"] = data["Location"].apply(lambda x: "US" if x=="US" else "US_" + x[:2])

>>>>>>> aeb73b3c8 (Vaccination by type:)
>>>>>>> e2f8dcce6 (Vaccination by type:)
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
