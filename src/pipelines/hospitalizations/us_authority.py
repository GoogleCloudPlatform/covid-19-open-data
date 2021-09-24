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


class HHSDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(
            dataframes[0],
            {
                "state": "subregion1_code",
                "date": "date",
                # "critical_staffing_shortage_today_yes": "",
                # "critical_staffing_shortage_today_no": "",
                # "critical_staffing_shortage_today_not_reported": "",
                # "critical_staffing_shortage_anticipated_within_week_yes": "",
                # "critical_staffing_shortage_anticipated_within_week_no": "",
                # "critical_staffing_shortage_anticipated_within_week_not_reported": "",
                # "hospital_onset_covid": "",
                # "hospital_onset_covid_coverage": "",
                # "inpatient_beds": "",
                # "inpatient_beds_coverage": "",
                # "inpatient_beds_used": "",
                # "inpatient_beds_used_coverage": "",
                # "inpatient_beds_used_covid": "",
                # "inpatient_beds_used_covid_coverage": "",
                "previous_day_admission_adult_covid_confirmed": "_new_hospitalized_adult",
                # "previous_day_admission_adult_covid_confirmed_coverage": "",
                # "previous_day_admission_adult_covid_suspected": "",
                # "previous_day_admission_adult_covid_suspected_coverage": "",
                "previous_day_admission_pediatric_covid_confirmed": "_new_hospitalized_pediatric",
                # "previous_day_admission_pediatric_covid_confirmed_coverage": "",
                # "previous_day_admission_pediatric_covid_suspected": "",
                # "previous_day_admission_pediatric_covid_suspected_coverage": "",
                # "staffed_adult_icu_bed_occupancy": "",
                # "staffed_adult_icu_bed_occupancy_coverage": "",
                # "staffed_icu_adult_patients_confirmed_and_suspected_covid": "",
                # "staffed_icu_adult_patients_confirmed_and_suspected_covid_coverage": "",
                "staffed_icu_adult_patients_confirmed_covid": "current_intensive_care",
                # "staffed_icu_adult_patients_confirmed_covid_coverage": "",
                # "total_adult_patients_hospitalized_confirmed_and_suspected_covid": "",
                # "total_adult_patients_hospitalized_confirmed_and_suspected_covid_coverage": "",
                "total_adult_patients_hospitalized_confirmed_covid": "_current_hospitalized_adult",
                # "total_adult_patients_hospitalized_confirmed_covid_coverage": "",
                # "total_pediatric_patients_hospitalized_confirmed_and_suspected_covid": "",
                # "total_pediatric_patients_hospitalized_confirmed_and_suspected_covid_coverage": "",
                "total_pediatric_patients_hospitalized_confirmed_covid": "_current_hospitalized_pediatric",
                # "total_pediatric_patients_hospitalized_confirmed_covid_coverage": "",
                # "total_staffed_adult_icu_beds": "",
                # "total_staffed_adult_icu_beds_coverage": "",
                # "inpatient_beds_utilization": "",
                # "inpatient_beds_utilization_coverage": "",
                # "inpatient_beds_utilization_numerator": "",
                # "inpatient_beds_utilization_denominator": "",
                # "percent_of_inpatients_with_covid": "",
                # "percent_of_inpatients_with_covid_coverage": "",
                # "percent_of_inpatients_with_covid_numerator": "",
                # "percent_of_inpatients_with_covid_denominator": "",
                # "inpatient_bed_covid_utilization": "",
                # "inpatient_bed_covid_utilization_coverage": "",
                # "inpatient_bed_covid_utilization_numerator": "",
                # "inpatient_bed_covid_utilization_denominator": "",
                # "adult_icu_bed_covid_utilization": "",
                # "adult_icu_bed_covid_utilization_coverage": "",
                # "adult_icu_bed_covid_utilization_numerator": "",
                # "adult_icu_bed_covid_utilization_denominator": "",
                # "adult_icu_bed_utilization": "",
                # "adult_icu_bed_utilization_coverage": "",
                # "adult_icu_bed_utilization_numerator": "",
                # "adult_icu_bed_utilization_denominator": "",
                # "geocoded_state": "",
                # "previous_day_admission_adult_covid_confirmed_18-19": "",
                # "previous_day_admission_adult_covid_confirmed_18-19_coverage": "",
                # "previous_day_admission_adult_covid_confirmed_20-29": "",
                # "previous_day_admission_adult_covid_confirmed_20-29_coverage": "",
                # "previous_day_admission_adult_covid_confirmed_30-39": "",
                # "previous_day_admission_adult_covid_confirmed_30-39_coverage": "",
                # "previous_day_admission_adult_covid_confirmed_40-49": "",
                # "previous_day_admission_adult_covid_confirmed_40-49_coverage": "",
                # "previous_day_admission_adult_covid_confirmed_50-59": "",
                # "previous_day_admission_adult_covid_confirmed_50-59_coverage": "",
                # "previous_day_admission_adult_covid_confirmed_60-69": "",
                # "previous_day_admission_adult_covid_confirmed_60-69_coverage": "",
                # "previous_day_admission_adult_covid_confirmed_70-79": "",
                # "previous_day_admission_adult_covid_confirmed_70-79_coverage": "",
                # "previous_day_admission_adult_covid_confirmed_80+": "",
                # "previous_day_admission_adult_covid_confirmed_80+_coverage": "",
                # "previous_day_admission_adult_covid_confirmed_unknown": "",
                # "previous_day_admission_adult_covid_confirmed_unknown_coverage": "",
                # "previous_day_admission_adult_covid_suspected_18-19": "",
                # "previous_day_admission_adult_covid_suspected_18-19_coverage": "",
                # "previous_day_admission_adult_covid_suspected_20-29": "",
                # "previous_day_admission_adult_covid_suspected_20-29_coverage": "",
                # "previous_day_admission_adult_covid_suspected_30-39": "",
                # "previous_day_admission_adult_covid_suspected_30-39_coverage": "",
                # "previous_day_admission_adult_covid_suspected_40-49": "",
                # "previous_day_admission_adult_covid_suspected_40-49_coverage": "",
                # "previous_day_admission_adult_covid_suspected_50-59": "",
                # "previous_day_admission_adult_covid_suspected_50-59_coverage": "",
                # "previous_day_admission_adult_covid_suspected_60-69": "",
                # "previous_day_admission_adult_covid_suspected_60-69_coverage": "",
                # "previous_day_admission_adult_covid_suspected_70-79": "",
                # "previous_day_admission_adult_covid_suspected_70-79_coverage": "",
                # "previous_day_admission_adult_covid_suspected_80+": "",
                # "previous_day_admission_adult_covid_suspected_80+_coverage": "",
                # "previous_day_admission_adult_covid_suspected_unknown": "",
                # "previous_day_admission_adult_covid_suspected_unknown_coverage": "",
                # "deaths_covid": "",
                # "deaths_covid_coverage": "",
                # "on_hand_supply_therapeutic_a_casirivimab_imdevimab_courses": "",
                # "on_hand_supply_therapeutic_b_bamlanivimab_courses": "",
                # "on_hand_supply_therapeutic_c_bamlanivimab_etesevimab_courses": "",
                # "previous_week_therapeutic_a_casirivimab_imdevimab_courses_used": "",
                # "previous_week_therapeutic_b_bamlanivimab_courses_used": "",
                # "previous_week_therapeutic_c_bamlanivimab_etesevimab_courses_used": "",
                # "icu_patients_confirmed_influenza": "",
                # "icu_patients_confirmed_influenza_coverage": "",
                # "previous_day_admission_influenza_confirmed": "",
                # "previous_day_admission_influenza_confirmed_coverage": "",
                # "previous_day_deaths_covid_and_influenza": "",
                # "previous_day_deaths_covid_and_influenza_coverage": "",
                # "previous_day_deaths_influenza": "",
                # "previous_day_deaths_influenza_coverage": "",
                # "total_patients_hospitalized_confirmed_influenza": "",
                # "total_patients_hospitalized_confirmed_influenza_and_covid": "",
                # "total_patients_hospitalized_confirmed_influenza_and_covid_coverage": "",
                # "total_patients_hospitalized_confirmed_influenza_coverage": "",
            },
            drop=True,
        )

        # Combine adult and pediatric statistics
        data["new_hospitalized"] = (
            data["_new_hospitalized_adult"] + data["_new_hospitalized_pediatric"]
        )
        data["current_hospitalized"] = (
            data["_current_hospitalized_adult"] + data["_current_hospitalized_pediatric"]
        )

        data["key"] = "US_" + data["subregion1_code"]
        data["date"] = data["date"].str.replace("/", "-")
        return data
