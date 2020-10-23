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

from typing import Dict
from pandas import DataFrame
from lib.pipeline import DataSource
from lib.utils import table_rename
from lib.time import datetime_isoformat
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

# Source: https://www.opendata.nhs.scot/en_AU/dataset/cbd1802e-0e04-4282-88eb-d7bdcfb120f0/resource/c698f450-eeed-41a0-88f7-c1e40a568acc
scotland_utla_to_nhs_board_map = {
    "S12000005": "S08000019",
    "S12000006": "S08000017",
    "S12000008": "S08000015",
    "S12000010": "S08000024",
    "S12000011": "S08000031",
    "S12000013": "S08000028",
    "S12000014": "S08000019",
    "S12000017": "S08000022",
    "S12000018": "S08000031",
    "S12000019": "S08000024",
    "S12000020": "S08000020",
    "S12000021": "S08000015",
    "S12000023": "S08000025",
    "S12000026": "S08000016",
    "S12000027": "S08000026",
    "S12000028": "S08000015",
    "S12000029": "S08000032",
    "S12000030": "S08000019",
    "S12000033": "S08000020",
    "S12000034": "S08000020",
    "S12000035": "S08000022",
    "S12000036": "S08000024",
    "S12000038": "S08000031",
    "S12000039": "S08000031",
    "S12000040": "S08000024",
    "S12000041": "S08000030",
    "S12000042": "S08000030",
    "S12000045": "S08000031",
    "S12000047": "S08000029",
    "S12000048": "S08000030",
    "S12000049": "S08000031",
    "S12000050": "S08000032",
}


def _fix_bad_total_deceased(data) -> None:
    # Some areas have bad reporting e.g. UKC on 09/09/2020 has N/A and 0 as
    # new_deceased and total_deceased, creating an erroneous entry in the data.
    filter_query = (data.new_deceased.isna()) & (data.total_deceased == 0)
    data.loc[filter_query, "total_deceased"] = None


class Covid19UkRegionsDataSource(DataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        # Regions have case data on a "by specimen date" basis.
        # This means they don't add up to the counts for nation which are on a
        # "by publish date" basis.
        cases_and_deaths = {
            "date": "date",
            "areaCode": "areaCode",
            "newCasesBySpecimenDate": "newCasesBySpecimenDate",
            "cumCasesBySpecimenDate": "cumCasesBySpecimenDate",
            "newDeaths28DaysByDeathDate": "newDeaths28DaysByDeathDate",
            "cumDeaths28DaysByDeathDate": "cumDeaths28DaysByDeathDate",
        }

        api = Cov19API(filters=["areaType=region"], structure=cases_and_deaths)
        data = api.get_dataframe()
        data = table_rename(
            data,
            {
                "areaCode": "match_string",
                "newCasesBySpecimenDate": "new_confirmed",
                "cumCasesBySpecimenDate": "total_confirmed",
                "newDeaths28DaysByDeathDate": "new_deceased",
                "cumDeaths28DaysByDeathDate": "total_deceased",
                "date": "date",
            },
            drop=True,
        )

        data.date = data.date.apply(lambda x: datetime_isoformat(x, "%Y-%m-%d"))
        _fix_bad_total_deceased(data)

        # Make sure all records have country code and no subregion code
        data["country_code"] = "GB"
        data["subregion2_code"] = None
        data["locality_code"] = None

        return data


class Covid19UkL2DataSource(DataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:

        cases_and_deaths_and_tests = {
            "date": "date",
            "areaCode": "areaCode",
            "newCasesBySpecimenDate": "newCasesBySpecimenDate",
            "cumCasesBySpecimenDate": "cumCasesBySpecimenDate",
            "newDeaths28DaysByDeathDate": "newDeaths28DaysByDeathDate",
            "cumDeaths28DaysByDeathDate": "cumDeaths28DaysByDeathDate",
            "cumTestsByPublishDate": "cumTestsByPublishDate",
            "newTestsByPublishDate": "newTestsByPublishDate",
        }
        api = Cov19API(filters=["areaType=nation"], structure=cases_and_deaths_and_tests)
        data = api.get_dataframe()

        data = table_rename(
            data,
            {
                "areaCode": "match_string",
                "newCasesBySpecimenDate": "new_confirmed",
                "cumCasesBySpecimenDate": "total_confirmed",
                "newDeaths28DaysByDeathDate": "new_deceased",
                "cumDeaths28DaysByDeathDate": "total_deceased",
                "newTestsByPublishDate": "new_tested",
                "cumTestsByPublishDate": "total_tested",
                "date": "date",
            },
            drop=True,
        )

        data.date = data.date.apply(lambda x: datetime_isoformat(x, "%Y-%m-%d"))
        _fix_bad_total_deceased(data)

        # Make sure all records have country code and no subregion code
        data["country_code"] = "GB"
        data["subregion2_code"] = None
        data["locality_code"] = None

        return data


class Covid19UkL1DataSource(DataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        cases_and_deaths_and_tests = {
            "date": "date",
            "areaCode": "areaCode",
            "newCasesBySpecimenDate": "newCasesBySpecimenDate",
            "cumCasesBySpecimenDate": "cumCasesBySpecimenDate",
            "newDeaths28DaysByDeathDate": "newDeaths28DaysByDeathDate",
            "cumDeaths28DaysByDeathDate": "cumDeaths28DaysByDeathDate",
            "cumTestsByPublishDate": "cumTestsByPublishDate",
            "newTestsByPublishDate": "newTestsByPublishDate",
        }
        api = Cov19API(filters=["areaType=overview"], structure=cases_and_deaths_and_tests)
        data = api.get_dataframe()

        data = table_rename(
            data,
            {
                "areaCode": "country_code",
                "newCasesBySpecimenDate": "new_confirmed",
                "cumCasesBySpecimenDate": "total_confirmed",
                "newDeaths28DaysByDeathDate": "new_deceased",
                "cumDeaths28DaysByDeathDate": "total_deceased",
                "newTestsByPublishDate": "new_tested",
                "cumTestsByPublishDate": "total_tested",
                "date": "date",
            },
            drop=True,
        )

        data.date = data.date.apply(lambda x: datetime_isoformat(x, "%Y-%m-%d"))
        _fix_bad_total_deceased(data)

        # We know the key since it's country-level data
        data["key"] = "GB"

        # For consistency across countries, do not report confirmed / deceased counts since we get
        # that data from WHO / ECDC
        data = data[
            [
                "key",
                "date",
                "new_confirmed",
                "total_confirmed",
                "new_deceased",
                "total_deceased",
                "new_tested",
                "total_tested",
            ]
        ]

        return data


def _apply_area_code_map(x):
    if x in wales_utla_map:
        return wales_utla_map[x]
    elif x in scotland_utla_to_nhs_board_map:
        return scotland_utla_to_nhs_board_map[x]
    else:
        return x


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
        data = api.get_dataframe()

        data.areaCode = data.areaCode.apply(_apply_area_code_map)
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
