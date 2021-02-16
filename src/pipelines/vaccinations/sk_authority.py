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
from typing import Any, Dict, List, Callable, Optional

from pandas import DataFrame, merge
import requests
import uuid
import json

from lib.data_source import DataSource
from lib.time import date_range, date_today, date_offset, datetime_isoformat
from lib.utils import table_rename


def _download_from_api(
    url: str, offset: Optional[int] = None, log_func: Callable[[str], None] = None
) -> List[Dict[str, Any]]:
    """
    Recursively download all records from data source's API respecting the maximum record
    transfer per request.
    """
    if offset:
        url_tpl = url + "?offset={offset}"
        url_fmt = url_tpl.format(offset=offset)
    else:
        url_fmt = url
    get_opts = dict(timeout=60)

    try:
        res = requests.get(url_fmt, **get_opts)
        if res.status_code != 200:
            raise ConnectionError(f"Could not connect to {url}")
        rows = res.json()
    except Exception as exc:
        if log_func:
            log_func(exc)
        raise exc

    if offset is None:
        # This is a non-recursive call.
        return rows

    if "next_offset" not in rows or rows["next_offset"] == None:
        return rows["page"]
    else:
        return rows["page"] + _download_from_api(url, offset=rows["next_offset"], log_func=log_func)


class SlovakiaDataSource(DataSource):
    def fetch(
        self,
        output_folder: Path,
        cache: Dict[str, str],
        fetch_opts: List[Dict[str, Any]],
        skip_existing: bool = False,
    ) -> Dict[str, str]:

        data_files = {}
        for fetch_opt in fetch_opts:
            # Base URL comes from fetch_opts
            url_base = fetch_opt["url"]
            name = fetch_opt["name"]

            # Create a deterministic file name
            file_path = (
                output_folder
                / "snapshot"
                / ("%s.%s" % (uuid.uuid5(uuid.NAMESPACE_DNS, url_base), "json"))
            )
            # Avoid download if the file exists and flag is set
            if not skip_existing or not file_path.exists():
                with open(file_path, "w") as fd:
                    if name == "regions":
                        json.dump(_download_from_api(url_base, log_func=self.log_error), fd)
                    elif name == "vaccine_by_regions":
                        json.dump(
                            _download_from_api(url_base, offset=0, log_func=self.log_error), fd
                        )
            data_files[name] = str(file_path.absolute())

        return data_files

    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        if "regions" not in dataframes or "vaccine_by_regions" not in dataframes:
            raise ArgumentError("Missing keys in dataframes dict.")

        data = merge(
            dataframes["vaccine_by_regions"],
            dataframes["regions"],
            left_on="region_id",
            right_on="id",
            how="left",
        ).reindex()

        data = table_rename(
            data,
            {
                "title": "subregion1_name",
                "published_on": "date",
                "dose1_count": "new_persons_vaccinated",
                "dose1_sum": "total_persons_vaccinated",
                "dose2_count": "new_persons_fully_vaccinated",
                "dose2_sum": "total_persons_fully_vaccinated",
            },
            drop=True,
        )

        data["new_vaccine_doses_administered"] = (
            data["new_persons_vaccinated"] + data["new_persons_fully_vaccinated"]
        )
        data["total_vaccine_doses_administered"] = (
            data["total_persons_vaccinated"] + data["total_persons_fully_vaccinated"]
        )

        data["country_code"] = "SK"
        data["subregion2_code"] = None
        data["locality_code"] = None

        return data
