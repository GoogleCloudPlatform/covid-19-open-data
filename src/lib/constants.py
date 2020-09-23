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
from pathlib import Path

SRC = Path(os.path.dirname(__file__)) / ".."
GCS_BUCKET_TEST = "open-covid-data"
GCS_BUCKET_PROD = "covid19-open-data"
GCS_CONTAINER_ID = "gcr.io/github-open-covid-19/runtime:latest"
URL_OUTPUTS_PROD = f"https://storage.googleapis.com/{GCS_BUCKET_PROD}/v2"
CACHE_URL = "https://raw.githubusercontent.com/open-covid-19/data/cache"
ISSUES_API_URL = "https://api.github.com/repos/GoogleCloudPlatform/covid-19-open-data/issues"

# Progress is a global flag, because progress is all done using the tqdm library and can be
# used within any number of functions but passing a flag around everywhere is cumbersome. Further,
# it needs to be an environment variable since the global module variables are reset across
# different processes.
GLOBAL_DISABLE_PROGRESS = "TQDM_DISABLE"

# Some tables are not included into the main CSV table
EXCLUDE_FROM_MAIN_TABLE = (
    "main",
    "index",
    "worldbank",
    "worldpop",
    "by-age",
    "by-sex",
    "search-trends-daily",
    "search-trends-weekly",
)

# Converts the outputs to the latest schema. Changing the config.yaml directly is not feasible for
# many tables because internal methods depend on their names (such as starting with "total_").
OUTPUT_COLUMN_ADAPTER = {
    # All tables
    "key": "location_key",
    # Index table
    "wikidata": "wikidata_id",
    "datacommons": "datacommons_id",
    "open_street_maps": "openstreetmap_id",
    "3166-1-alpha-2": "iso_3166_1_alpha_2",
    "3166-1-alpha-3": "iso_3166_1_alpha_3",
    # Epidemiology table
    "total_confirmed": "cumulative_confirmed",
    "total_deceased": "cumulative_deceased",
    "total_recovered": "cumulative_recovered",
    "total_tested": "cumulative_tested",
    # Hospitalizations table
    "total_hospitalized": "cumulative_hospitalized",
    "total_intensive_care": "cumulative_intensive_care",
    "total_ventilator": "cumulative_ventilator",
    # Demographics table
    "rural_population": "population_rural",
    "urban_population": "population_urban",
    "largest_city_population": "population_largest_city",
    "clustered_population": "population_clustered",
    "population_age_80_89": None,
    "population_age_90_99": None,
    # Economics table
    "gdp": "gdp_usd",
    "gdp_per_capita": "gdp_per_capita_usd",
    # Geography table
    "elevation": "elevation_meters",
    "area": "area_squared_kilometers",
    "rural_area": "rural_area_squared_kilometers",
    "urban_area": "urban_area_squared_kilometers",
    # Health
    "hospital_beds": "hospital_beds_per_1000",
    "nurses": "nurses_per_1000",
    "physicians": "physicians_per_1000",
    "health_expenditure": "health_expenditure_usd",
    "out_of_pocket_health_expenditure": "out_of_pocket_health_expenditure_usd",
    # Weather
    "noaa_station": None,
    "noaa_distance": None,
    "average_temperature": "average_temperature_celsius",
    "minimum_temperature": "minimum_temperature_celsius",
    "maximum_temperature": "maximum_temperature_celsius",
    "rainfall": "rainfall_millimeters",
    "snowfall": "snowfall_millimeters",
}
