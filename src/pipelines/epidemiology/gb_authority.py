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

from pathlib import Path
from typing import Any, Dict, List
from pandas import DataFrame, concat
from lib.pipeline import DataSource
from lib.utils import table_rename
from lib.time import datetime_isoformat
from lib.cast import safe_int_cast
import requests
from uk_covid19 import Cov19API


# Maps unitary authority (UTLA) to Local Health Board in Wales
# source : https://data.gov.uk/dataset/0ae307c2-3092-41e7-a3d0-5d268c8d3662/unitary-authority-to-local-health-board-april-2019-lookup-in-wales
wales_utla_map = {
    "W06000001": "W11000023",
    "W06000002": "W11000023",
    "W06000003": "W11000023",
    "W06000004": "W11000023",
    "W06000005": "W11000023",
    "W06000006": "W11000023",
    "W06000023": "W11000024",
    "W06000008": "W11000025",
    "W06000009": "W11000025",
    "W06000010": "W11000025",
    "W06000018": "W11000028",
    "W06000019": "W11000028",
    "W06000020": "W11000028",
    "W06000021": "W11000028",
    "W06000022": "W11000028",
    "W06000014": "W11000029",
    "W06000015": "W11000029",
    "W06000013": "W11000030",
    "W06000016": "W11000030",
    "W06000024": "W11000030",
    "W06000011": "W11000031",
    "W06000012": "W11000031",
}


class Covid19UkL2DataSource(DataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        cases_and_deaths = {
            "date": "date",
            "areaName": "areaName",
            "newCasesByPublishDate": "newCasesByPublishDate",
            "cumCasesByPublishDate": "cumCasesByPublishDate",
            "newDeathsByPublishDate": "newDeathsByPublishDate",
            "cumDeathsByPublishDate": "cumDeathsByPublishDate",
            "cumPillarOneTestsByPublishDate": "cumPillarOneTestsByPublishDate",
        }
        api = Cov19API(filters=["areaType=nation"], structure=cases_and_deaths)
        data_json = api.get_json()
        data = DataFrame.from_dict(data_json["data"])

        data = table_rename(
            data,
            {
                "areaName": "subregion1_name",
                "newCasesByPublishDate": "new_confirmed",
                "cumCasesByPublishDate": "total_confirmed",
                "newDeathsByPublishDate": "new_deceased",
                "cumDeathsByPublishDate": "total_deceased",
                "cumPillarOneTestsByPublishDate": "total_tested",
                "date": "date",
            },
            drop=True,
        )

        data = data[data["subregion1_name"] != "UK"]
        data.date = data.date.apply(lambda x: datetime_isoformat(x, "%Y-%m-%d"))

        # Make sure all records have country code and no subregion code
        data["country_code"] = "GB"
        data["subregion2_code"] = None

        return data


class Covid19UkL1DataSource(Covid19UkL2DataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        data = super().parse(sources, aux, **parse_opts)

        # Aggregate data to the country level
        data = data.groupby("date").sum().reset_index()
        data["key"] = "GB"
        return data


class Covid19UkL3DataSource(DataSource):
    """ This API can be used to get historical case data for utla and ltla in the UK """

    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        cases = {
            "date": "date",
            "areaCode": "areaCode",
            "newCasesBySpecimenDate": "newCasesBySpecimenDate",
            "cumCasesBySpecimenDate": "cumCasesBySpecimenDate",
        }

        api = Cov19API(filters=["areaType=utla"], structure=cases)
        utla_json = api.get_json()
        data = DataFrame.from_dict(utla_json["data"])

        data.areaCode = data.areaCode.apply(
            lambda x: wales_utla_map[x] if x in wales_utla_map else x
        )
        data = data.groupby(["date", "areaCode"], as_index=False).sum()

        data = table_rename(
            data,
            {
                "areaCode": "subregion2_code",
                "newCasesBySpecimenDate": "new_confirmed",
                "cumCasesBySpecimenDate": "total_confirmed",
                "date": "date",
            },
            drop=True,
        )

        data.date = data.date.apply(lambda x: datetime_isoformat(x, "%Y-%m-%d"))

        return data
