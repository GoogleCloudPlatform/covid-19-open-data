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

from typing import Any, Dict
from pandas import DataFrame, concat
from lib.case_line import convert_cases_to_time_series
from lib.data_source import DataSource
from lib.utils import table_rename
from lib.utils import table_merge


_column_adapter = {
    "date": "date",
    "state": "subregion1_name",
    #
    # Epidemiology
    #
    "deaths_new": "new_deceased",
    "cases_new": "new_confirmed",
    "cases_recovered": "new_recovered",
    "rtk-ag": "new_tested_1",
    "pcr": "new_tested_2",
    #
    # Hospitalizations
    #
    "admitted_covid": "new_hospitalized",
    "hosp_covid": "current_hospitalized",
    "icu_covid": "current_intensive_care",
    "vent_covid": "current_ventilator",
    #
    # Vaccinations
    #
    "daily_partial": "new_persons_vaccinated",
    "daily_full": "new_persons_fully_vaccinated",
    "daily": "new_vaccine_doses_administered",
    # "daily_partial_child": '',
    # "daily_full_child": '',
    # "daily_booster": '',
    "cumul_partial": "total_persons_vaccinated",
    "cumul_full": "total_persons_fully_vaccinated",
    "cumul": "total_vaccine_doses_administered",
    # "cumul_partial_child": '',
    # "cumul_full_child": '',
    # "cumul_booster": '',
    "pfizer1": "new_persons_vaccinated_pfizer",
    "pfizer2": "new_persons_fully_vaccinated_pfizer",
    # "pfizer3": '',
    "sinovac1": "new_persons_vaccinated_sinovac",
    "sinovac2": "total_persons_vaccinated_sinovac",
    # "sinovac3": '',
    "astra1": "new_persons_vaccinated_astrazeneca",
    "astra2": "new_persons_fully_vaccinated_astrazeneca",
    # "astra3": '',
    # "sinopharm1": 'new_persons_vaccinated_sinopharm',
    # "sinopharm2": 'new_persons_fully_vaccinated_sinopharm',
    # "sinopharm3": '',
    # "cansino": 'new_persons_vaccinated_cansino',
    # "cansino3": '',
    # "pending1": '',
    # "pending2": '',
    # "pending3": '',
    #
    # Line List
    #
    "age": "age",
    "male": "sex",
}


def _process_inputs(dataframes: Dict[Any, DataFrame]) -> DataFrame:
    # Combine all tables
    data = table_merge(dataframes.values(), how="outer")
    data = table_rename(data, _column_adapter, drop=True)
    data["country_code"] = "MY"

    # Remove records with no date
    data = data.dropna(subset=["date"])

    # Fix the subregion names to match our index
    if "subregion1_name" in data.columns:
        data["subregion1_name"] = data["subregion1_name"].str.replace("W.P. ", "")

    # Add up different categories
    if "new_tested_1" in data.columns and "new_tested_2" in data.columns:
        data["new_tested"] = data["new_tested_1"] + data["new_tested_2"]

    return data


class MalaysiaCountryDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = _process_inputs(dataframes)
        data["key"] = "MY"
        return data


class MalaysiaStateDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = _process_inputs(dataframes)
        data["subregion2_name"] = None
        return data


class MalaysiaDeathsLineListDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        column_adapter = dict(_column_adapter, date=f"date_new_deceased")
        data = table_rename(dataframes[0], column_adapter=column_adapter, drop=True)

        data["sex"] = data["sex"].apply({0: "female", 1: "male"}.get)
        data["age"] = data["age"].apply(lambda x: None if x < 0 else x)
        data["subregion1_name"] = data["subregion1_name"].str.replace("W.P. ", "")
        data = convert_cases_to_time_series(data, ["subregion1_name"])

        # Remove records with no location
        data = data.dropna(subset=["subregion1_name"])

        data["country_code"] = "MY"
        data["subregion2_code"] = None
        return data


class MalaysiaCasesLineListDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[Any, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        tables = [df for name, df in dataframes.items() if name != "geo"]
        column_adapter = dict(_column_adapter, state="idxs", date=f"date_new_confirmed")
        data = table_rename(concat(tables), column_adapter=column_adapter, drop=True)

        # Correct data types where necessary
        data["idxs"] = data["idxs"].astype(str)
        data["age"] = data["age"].apply(lambda x: None if x < 0 else x)
        data["sex"] = data["sex"].apply({0: "female", 1: "male"}.get)

        # Convert to our preferred time series format
        data = convert_cases_to_time_series(data, ["idxs"])

        # Geo name lookup
        geo_col_adapter = {"state": "subregion1_name", "district": "subregion2_name"}
        geo = table_rename(dataframes["geo"], geo_col_adapter, drop=False)
        geo["idxs"] = geo["idxs"].astype(str)
        geo["subregion1_name"] = geo["subregion1_name"].str.replace("W.P. ", "")
        geo = geo.groupby(["subregion1_name", "idxs"]).first().reset_index()
        data = table_merge([data, geo], on=["idxs"], how="inner")

        # Since only the cases have district level data, ignore it
        data["country_code"] = "MY"
        data["subregion2_name"] = None
        return data
