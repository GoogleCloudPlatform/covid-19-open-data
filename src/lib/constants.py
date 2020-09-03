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
URL_OUTPUTS_PROD = f"https://storage.googleapis.com/{GCS_BUCKET_PROD}/v2"
CACHE_URL = "https://raw.githubusercontent.com/open-covid-19/data/cache"
ISSUES_API_URL = "https://api.github.com/repos/GoogleCloudPlatform/covid-19-open-data/issues"


# Some tables are not included into the main table
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
