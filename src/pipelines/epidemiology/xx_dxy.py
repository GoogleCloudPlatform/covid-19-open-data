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

import datetime
from pathlib import Path
from typing import Any, Dict, List

import requests
from pandas import DataFrame

from lib.data_source import DataSource
from lib.time import date_range, timezone_adjust


class DXYDataSource(DataSource):
    def fetch(
        self,
        output_folder: Path,
        cache: Dict[str, str],
        fetch_opts: List[Dict[str, Any]],
        skip_existing: bool = False,
    ) -> Dict[str, str]:
        # Data is published as GitHub Releases, so we guess the URL based on the date
        opts = fetch_opts[0]
        url_tpl = opts["url"]

        # Go from <today + 1> until the last known date for which data is reported
        # NOTE: at the time of writing, last known date is October 20
        working_url = None
        last_known_date = "2020-10-20"
        latest_date = (datetime.datetime.today() + datetime.timedelta(days=1)).date().isoformat()
        for date in reversed(list(date_range(last_known_date, latest_date))):
            try:
                url_test = url_tpl.format(date=date.replace("-", "."))
                self.log_debug(f"Trying {url_test}")
                res = requests.get(url_test, timeout=60)
                if res.ok:
                    working_url = url_test
                    break
            except:
                continue

        # Make sure that we found a working URL
        assert working_url is not None, f"No working URL found for DXY data source"

        # Pass the actual URL down to fetch it
        return super().fetch(
            output_folder, cache, [{**opts, "url": working_url}], skip_existing=skip_existing
        )

    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = dataframes[0]

        # Adjust 7 hour difference between China's GMT+8 and GMT+1
        data["date"] = data["updateTime"].apply(lambda date: timezone_adjust(date, 7))

        # Rename the appropriate columns
        data = data.rename(
            columns={
                "countryEnglishName": "country_name",
                "provinceEnglishName": "match_string",
                "province_confirmedCount": "total_confirmed",
                "province_deadCount": "total_deceased",
                "province_curedCount": "total_recovered",
            }
        )

        # Filter specific country data only
        data = data[data["country_name"] == parse_opts["country_name"]]

        # This is time series data, get only the last snapshot of each day
        data = (
            data.sort_values("updateTime")
            .groupby(["date", "country_name", "match_string"])
            .last()
            .reset_index()
        )

        # A couple of regions are reported using conflicting country codes, harmonize them here so
        # we avoid repeated regions
        data["key"] = None
        data.loc[data["match_string"] == "Taiwan", "key"] = "TW"
        data.loc[data["match_string"] == "Hong Kong", "key"] = "HK"
        data.loc[data["match_string"] == "Macau", "key"] = "MO"

        keep_columns = [
            "key",
            "date",
            "country_name",
            "match_string",
            "total_confirmed",
            "total_deceased",
            "total_recovered",
        ]
        return data[keep_columns]
