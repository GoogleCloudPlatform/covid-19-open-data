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

from pathlib import Path
from typing import Any, Dict, List
import requests
from pandas import DataFrame
from lib.data_source import DataSource
from lib.utils import table_rename


class SwitzerlandDataSource(DataSource):
    def fetch(
            self,
            output_folder: Path,
            cache: Dict[str, str],
            fetch_opts: List[Dict[str, Any]],
            skip_existing: bool = False,
    ) -> Dict[str, str]:
        # the url is the config is a json file which contains the actual url for vaccDosesAdministered
        src_url = fetch_opts[0]['url']
        data = requests.get(src_url).json()
        vaccines_url = data['sources']['individual']['csv']['vaccDosesAdministered']
        fetch_opts[0]['url'] = vaccines_url
        return super().fetch(output_folder, cache, fetch_opts, skip_existing)

    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = table_rename(
            dataframes[0],
            {
                "date": "date",
                "geoRegion": "subregion1_code",
                "sumTotal": "total_vaccine_doses_administered",
            },
            drop=True,
        )

        # Make sure all records have the country code and match subregion1 only
        data["key"] = None
        data["country_code"] = "CH"
        data["subregion2_code"] = None
        data["locality_code"] = None

        # Country-level records have a known key
        country_mask = data["subregion1_code"] == "CH"
        data.loc[country_mask, "key"] = "CH"

        # Output the results
        return data
