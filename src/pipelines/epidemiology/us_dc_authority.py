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

from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List
from bs4 import BeautifulSoup
from pandas import DataFrame
from lib.cast import safe_float_cast
from lib.io import read_file
from lib.net import download_snapshot, download
from lib.pipeline import DataSource
from lib.time import datetime_isoformat
from lib.utils import pivot_table_date_columns, table_rename


def _preprocess_age_sex(data: DataFrame) -> DataFrame:
    data = data.iloc[1:].set_index(data.columns[0])
    return pivot_table_date_columns(data, value_name="total_deceased").reset_index()


def _parse_sex(data: DataFrame) -> DataFrame:
    data = _preprocess_age_sex(data)
    data = data.rename(columns={"index": "sex"})
    data = data[data.sex != "All"]
    data.sex = data.sex.apply({"Male": "male", "Female": "female"}.get)
    return data


def _parse_age(data: DataFrame) -> DataFrame:
    data = _preprocess_age_sex(data)
    data = data.rename(columns={"index": "age"})
    data = data[data.age != "All"]
    data.age = data.age.str.replace("<", "0-")
    data.age = data.age.str.replace("+", "-")
    return data


def _parse_summary(data: DataFrame) -> DataFrame:
    data = data[data.columns[1:]]
    data.columns = ["statistic"] + list(data.columns[1:])
    data = data.dropna(subset=data.columns[1:], how="all")

    data = pivot_table_date_columns(data.set_index("statistic"), value_name="statistic")
    data = data.reset_index().dropna(subset=["date"])
    data.statistic = data.statistic.apply(safe_float_cast).astype(float)

    data = data.pivot_table(index="date", columns=["index"], values="statistic")
    data = data.reset_index()

    data = table_rename(
        data,
        {
            "date": "date",
            "Total Positives": "total_confirmed",
            "Number of Deaths": "total_deceased",
            "Total Overall Tested": "total_tested",
            "Cleared From Isolation": "total_recovered",
            "Total COVID-19 Patients in DC Hospitals": "total_hospitalized",
            "Total COVID-19 Patients in ICU": "total_intensive_care",
        },
        drop=True,
    )
    return data


_sheet_processors = {
    "Overal Stats": _parse_summary,
    "Lives Lost by Age": _parse_age,
    "Lives Lost by Sex": _parse_sex,
}


class DistrictColumbiaDataSource(DataSource):
    def fetch(
        self, output_folder: Path, cache: Dict[str, str], fetch_opts: List[Dict[str, Any]]
    ) -> List[str]:
        # The link to the spreadsheet changes daily, so we parse the HTML to find the link every
        # time and download the latest version
        buffer = BytesIO()
        src_opts = fetch_opts[0]
        download(src_opts["url"], buffer)
        page = BeautifulSoup(buffer.getvalue().decode("utf8"), "lxml")
        for link in page.findAll("a"):
            if "href" in link.attrs and link.attrs.get("href").endswith("xlsx"):
                href = link.attrs.get("href")
                if href.startswith("/"):
                    href = "https://" + src_opts["url"].split("//")[1].split("/")[0] + href
                return [download_snapshot(href, output_folder, **src_opts.get("opts"))]
        raise RuntimeError("No link to XLSX file found in page")

    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        data = read_file(sources[0], sheet_name=parse_opts.get("sheet_name"))

        # Process the individual sheet
        data = _sheet_processors[parse_opts.get("sheet_name")](data)

        # Fix up the date format
        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%Y-%m-%d %H:%M:%S"))

        # Add a key to all the records (state-level only)
        data["key"] = "US_DC"
        return data
