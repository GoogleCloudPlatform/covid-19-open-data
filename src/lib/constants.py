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

# Project-specific constants
# TODO: Move to external INI file and read via configparser
GCS_BUCKET_TEST = "open-covid-data"
GCS_BUCKET_PROD = "covid19-open-data"
GCS_CONTAINER_ID = "gcr.io/github-open-covid-19/runtime:latest"
URL_OUTPUTS_PROD = f"https://storage.googleapis.com/{GCS_BUCKET_PROD}/v2"
CACHE_URL = f"https://storage.googleapis.com/{GCS_BUCKET_PROD}/cache"
ISSUES_API_URL = "https://api.github.com/repos/GoogleCloudPlatform/covid-19-open-data/issues"
GCE_IMAGE_ID = "cos-85-13310-1041-38"
GCE_IMAGE_PROJECT = "cos-cloud"
GCP_LOCATION = "us-east1"
GCP_ZONE = f"{GCP_LOCATION}-b"
GCLOUD_BIN = "/opt/google-cloud-sdk/bin/gcloud"
GCE_INSTANCE_TYPE = "n2-highmem-8"
GCP_SELF_DESTRUCT_SCRIPT = SRC / "scripts" / "startup-script-self-destruct.sh"


# Progress is a global flag, because progress is all done using the tqdm library and can be
# used within any number of functions but passing a flag around everywhere is cumbersome. Further,
# it needs to be an environment variable since the global module variables are reset across
# different processes.
GLOBAL_DISABLE_PROGRESS = "TQDM_DISABLE"

# Used to filter read_opts from a parse_opts argument
READ_OPTS = (
    "dtype",
    "encoding",
    "error_bad_lines",
    "file_name",
    "low_memory",
    "sep",
    "sheet_name",
    "usecols",
)

# Some tables are not included into the main CSV table
V2_TABLE_LIST = (
    "index",
    "epidemiology",
    "hospitalizations",
    "demographics",
    "economy",
    "geography",
    "health",
    "mobility",
    "oxford-government-response",
    "weather",
)
V3_TABLE_LIST = (
    "index",
    "epidemiology",
    "hospitalizations",
    "vaccinations",
    "by-age",
    "by-sex",
    "demographics",
    "economy",
    "geography",
    "health",
    "mobility",
    "oxford-government-response",
    "google-search-trends",
    "weather",
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
    # Vaccinations table
    "total_persons_vaccinated": "cumulative_persons_vaccinated",
    "total_persons_fully_vaccinated": "cumulative_persons_fully_vaccinated",
    "total_vaccine_doses_administered": "cumulative_vaccine_doses_administered",
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
    "elevation": "elevation_m",
    "area": "area_sq_km",
    "rural_area": "area_rural_sq_km",
    "urban_area": "area_urban_sq_km",
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
    "rainfall": "rainfall_mm",
    "snowfall": "snowfall_mm",
    # By Age
    "age_bin_00": "age_bin_0",
    "age_bin_01": "age_bin_1",
    "age_bin_02": "age_bin_2",
    "age_bin_03": "age_bin_3",
    "age_bin_04": "age_bin_4",
    "age_bin_05": "age_bin_5",
    "age_bin_06": "age_bin_6",
    "age_bin_07": "age_bin_7",
    "age_bin_08": "age_bin_8",
    "age_bin_09": "age_bin_9",
    **{
        f"{statistic}_age_{idx:02d}": f"{statistic.replace('total', 'cumulative')}_age_{idx:01d}"
        for idx in range(10)
        for statistic in (
            "new_confirmed",
            "new_deceased",
            "new_recovered",
            "new_tested",
            "new_hospitalized",
            "new_intensive_care",
            "new_ventilator",
            "total_confirmed",
            "total_deceased",
            "total_recovered",
            "total_tested",
            "total_hospitalized",
            "total_intensive_care",
            "total_ventilator",
        )
    },
    # By Sex
    **{
        f"{statistic}_{sex}": f"{statistic.replace('total', 'cumulative')}_{sex}"
        for sex in ("male", "female", "sex_other")
        for statistic in (
            "new_confirmed",
            "new_deceased",
            "new_recovered",
            "new_tested",
            "new_hospitalized",
            "new_intensive_care",
            "new_ventilator",
            "total_confirmed",
            "total_deceased",
            "total_recovered",
            "total_tested",
            "total_hospitalized",
            "total_intensive_care",
            "total_ventilator",
        )
    },
}
