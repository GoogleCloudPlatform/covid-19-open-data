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

import json
from typing import Dict
from pandas import DataFrame, concat
from lib.case_line import convert_cases_to_time_series
from lib.pipeline import DataSource
from lib.time import datetime_isoformat
from lib.utils import table_rename, aggregate_admin_level


_SUBREGION1_CODE_MAP = {
    "Harju": "37",
    "Hiiu": "39",
    "Ida-Viru": "45",
    "Jõgeva": "50",
    "Järva": "52",
    "Lääne": "56",
    "Lääne-Viru": "60",
    "Põlva": "64",
    "Pärnu": "68",
    "Rapla": "71",
    "Saare": "74",
    "Tartu": "79",
    "Valga": "81",
    "Viljandi": "84",
    "Võru": "87",
}


class ThailandCountryDataSource(DataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        with open(sources[0], "r") as fd:
            data = json.load(fd)["Data"]

        # "Date":"01\/01\/2020","NewConfirmed":0,"NewRecovered":0,"NewHospitalized":0,"NewDeaths":0,"Confirmed":0,"Recovered":0,"Hospitalized":0,"Deaths":0
        data = table_rename(
            DataFrame.from_records(data),
            {
                "Date": "date",
                "NewConfirmed": "new_confirmed",
                "NewRecovered": "new_recovered",
                "NewHospitalized": "new_hospitalized",
                "NewDeaths": "new_deceased",
                "Confirmed": "total__confirmed",
                "Recovered": "total__recovered",
                "Hospitalized": "total__hospitalized",
                "Deaths": "total__deceased",
            },
            drop=True,
            remove_regex=r"[^0-9a-z\s]",
        )

        # Format date as ISO date
        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%m/%d/%Y"))

        # Add key and return data
        data["key"] = "TH"
        return data


class ThailandProvinceDataSource(DataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        with open(sources[0], "r") as fd:
            cases = json.load(fd)["Data"]

        # {"ConfirmDate":"2021-01-09 00:00:00","No":"9876","Age":66,"Gender":"\u0e0a","GenderEn":"Male","Nation":"Thailand","NationEn":"Thailand","Province":"\u0e2d","ProvinceId":72,"District":"\u0e44","ProvinceEn":"Ang Thong","Detail":null,"StatQuarantine":1}
        cases = table_rename(
            DataFrame.from_records(cases),
            {
                "ConfirmDate": "date_new_confirmed",
                "Age": "age",
                "GenderEn": "sex",
                "ProvinceEn": "match_string",
            },
            drop=True,
        )

        # Convert dates to ISO format
        for col in cases.columns:
            if col.startswith("date_"):
                cases[col] = cases[col].str.slice(0, 10)

        # Parse age and sex fields
        cases["sex"] = cases["sex"].str.lower().apply({"male": "male", "female": "female"}.get)
        cases["age"] = cases["age"].fillna("age_unknown")
        cases["sex"] = cases["sex"].fillna("sex_unknown")

        # Convert to time series data
        data = convert_cases_to_time_series(cases, ["match_string"])

        # Aggregate by country level
        country = aggregate_admin_level(data, ["date", "age", "sex"], "country")
        country["key"] = "TH"

        # Add country code and return data
        data["country_code"] = "TH"
        data = data[data["match_string"] != "Unknown"]

        return concat([country, data])


class ThailandCasesDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        cases = table_rename(
            dataframes[0],
            {
                # "no": "",
                "age": "age",
                "sex": "sex",
                # "nationality": "",
                # "province_of_isolation": "",
                # "notification_date": "date",
                "announce_date": "date_new_confirmed",
                "province_of_onset": "match_string",
                # "district_of_onset": "subregion2_name",
                # "quarantine": "",
            },
            drop=True,
            remove_regex=r"[^0-9a-z\s]",
        )

        # Convert date to ISO format
        cases["date_new_confirmed"] = cases["date_new_confirmed"].str.slice(0, 10)

        # Some dates are not properly parsed, so fix those manually
        for col in (col for col in cases.columns if col.startswith("date_")):
            cases[col] = cases[col].str.replace("1963-", "2020-")
            cases[col] = cases[col].str.replace("2563-", "2020-")
            cases[col] = cases[col].str.replace("15/15/2020", "2020-12-15")
            cases[col] = cases[col].str.replace("15/15/2021", "2020-12-15")

        # Translate sex labels; only male, female and unknown are given
        sex_adapter = lambda x: {"ชาย": "male", "หญิง": "female"}.get(x, "sex_unknown")
        cases["sex"] = cases["sex"].apply(sex_adapter)

        # Convert from cases to time-series format
        data = convert_cases_to_time_series(cases, ["match_string"])

        # Aggregate country-level data by adding all counties
        country = (
            data.drop(columns=["match_string"]).groupby(["date", "age", "sex"]).sum().reset_index()
        )
        country["key"] = "TH"

        # Drop bogus records from the data
        data = data[data["match_string"].notna() & (data["match_string"] != "")]

        return concat([country, data])
