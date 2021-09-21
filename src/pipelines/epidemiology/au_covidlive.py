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

from typing import Dict
from pandas import DataFrame
from lib.data_source import DataSource
from lib.time import datetime_isoformat
from lib.utils import table_rename
from lib.vaccinations_utils import estimate_total_persons_vaccinated


_column_adapter = {
    "REPORT_DATE": "date",
    # "LAST_UPDATED_DATE": "",
    "CODE": "subregion1_code",
    "NAME": "subregion1_name",
    "CASE_CNT": "total_confirmed",
    "TEST_CNT": "total_tested",
    "DEATH_CNT": "total_deceased",
    "RECOV_CNT": "total_recovered",
    "MED_ICU_CNT": "current_intensive_care",
    "MED_VENT_CNT": "current_ventilator",
    "MED_HOSP_CNT": "current_hospitalized",
    # "SRC_OVERSEAS_CNT": "",
    # "SRC_INTERSTATE_CNT": "",
    # "SRC_CONTACT_CNT": "",
    # "SRC_UNKNOWN_CNT": "",
    # "SRC_INVES_CNT": "",
    # "PREV_CASE_CNT": "",
    # "PREV_TEST_CNT": "",
    # "PREV_DEATH_CNT": "",
    # "PREV_RECOV_CNT": "",
    # "PREV_MED_ICU_CNT": "",
    # "PREV_MED_VENT_CNT": "",
    # "PREV_MED_HOSP_CNT": "",
    # "PREV_SRC_OVERSEAS_CNT": "",
    # "PREV_SRC_INTERSTATE_CNT": "",
    # "PREV_SRC_CONTACT_CNT": "",
    # "PREV_SRC_UNKNOWN_CNT": "",
    # "PREV_SRC_INVES_CNT": "",
    # "PROB_CASE_CNT": "",
    # "PREV_PROB_CASE_CNT": "",
    # "ACTIVE_CNT": "",
    # "PREV_ACTIVE_CNT": "",
    "NEW_CASE_CNT": "new_confirmed",
    # "PREV_NEW_CASE_CNT": "",
    # "VACC_DIST_CNT": "",
    # "PREV_VACC_DIST_CNT": "",
    "VACC_DOSE_CNT": "total_vaccine_doses_administered",
    # "PREV_VACC_DOSE_CNT": "",
    "VACC_PEOPLE_CNT": "total_persons_fully_vaccinated",
    # "PREV_VACC_PEOPLE_CNT": "",
    # "VACC_AGED_CARE_CNT": "",
    # "PREV_VACC_AGED_CARE_CNT": "",
    # "VACC_GP_CNT": "",
    # "PREV_VACC_GP_CNT": "",
    # "VACC_FIRST_DOSE_CNT": "",
    # "PREV_VACC_FIRST_DOSE_CNT": "",
    # "VACC_FIRST_DOSE_CNT_12_15": "",
    # "PREV_VACC_FIRST_DOSE_CNT_12_15": "",
    # "VACC_PEOPLE_CNT_12_15": "",
    # "PREV_VACC_PEOPLE_CNT_12_15": "",
}


class AustraliaCovidLiveDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = table_rename(dataframes[0], _column_adapter, drop=True)

        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%Y-%m-%d"))

        # Country-level record has code AUS
        country_mask = data["subregion1_code"] == "AUS"
        data.loc[country_mask, "key"] = "AU"
        data.loc[~country_mask, "key"] = "AU_" + data.loc[~country_mask, "subregion1_code"]

        # based on the assumption two doses = fully vaccinated(since Australia is using Pfizer and AZ)
        data["total_persons_vaccinated"] = estimate_total_persons_vaccinated(data)

        return data
