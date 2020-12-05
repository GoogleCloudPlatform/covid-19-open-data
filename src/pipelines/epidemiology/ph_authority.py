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
from pandas import DataFrame, concat
from lib.case_line import convert_cases_to_time_series
from lib.cast import age_group, safe_datetime_parse
from lib.data_source import DataSource
from lib.utils import table_rename


class PhilippinesDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename appropriate columns
        cases = table_rename(
            dataframes[0],
            {
                "ProvRes": "province",
                "RegionRes": "region",
                "CityMuniPSGC": "city",
                "DateDied": "date_new_deceased",
                "DateSpecimen": "date_new_confirmed",
                "DateRecover": "date_new_recovered",
                "daterepconf": "_date_estimate",
                "admitted": "_hospitalized",
                "removaltype": "_prognosis",
                "Age": "age",
                "Sex": "sex",
            },
            drop=True,
        )

        # When there is a case, but missing confirmed date, estimate it
        cases["date_new_confirmed"] = cases["date_new_confirmed"].fillna(cases["_date_estimate"])

        # When there is recovered removal, but missing recovery date, estimate it
        nan_recovered_mask = cases.date_new_recovered.isna() & (cases["_prognosis"] == "Recovered")
        cases.loc[nan_recovered_mask, "date_new_recovered"] = cases.loc[
            nan_recovered_mask, "_date_estimate"
        ]

        # When there is deceased removal, but missing recovery date, estimate it
        nan_deceased_mask = cases.date_new_deceased.isna() & (cases["_prognosis"] == "Died")
        cases.loc[nan_deceased_mask, "date_new_deceased"] = cases.loc[
            nan_deceased_mask, "_date_estimate"
        ]

        # Hospitalized is estimated as the same date as confirmed if admitted == yes
        cases["date_new_hospitalized"] = None
        hospitalized_mask = cases["_hospitalized"].str.lower() == "yes"
        cases.loc[hospitalized_mask, "date_new_hospitalized"] = cases.loc[
            hospitalized_mask, "date_new_confirmed"
        ]

        # Create stratified age bands
        cases["age"] = cases["age"].apply(age_group)

        # Rename the sex values
        cases["sex"] = cases["sex"].apply({"MALE": "male", "FEMALE": "female"}.get)

        # Drop columns which we have no use for
        cases = cases[[col for col in cases.columns if not col.startswith("_")]]

        # NCR cases are broken down by city, not by province
        ncr_prov_mask = cases["region"] == "NCR"
        cases.loc[ncr_prov_mask, "province"] = cases.loc[ncr_prov_mask, "city"].str.slice(2, -3)
        cases.drop(columns=["city"], inplace=True)

        # Go from individual case records to key-grouped records in a flat table
        data = convert_cases_to_time_series(cases, index_columns=["province", "region"])

        # Convert date to ISO format
        data["date"] = data["date"].apply(safe_datetime_parse)
        data = data[~data["date"].isna()]
        data["date"] = data["date"].apply(lambda x: x.date().isoformat())

        # Null values are known to be zero, since we have case-line data
        data = data.fillna(0)

        # Aggregate country level directly from base data
        country = (
            data.drop(columns=["province", "region"])
            .groupby(["date", "age", "sex"])
            .sum()
            .reset_index()
        )
        country["key"] = "PH"

        # Aggregate regions and provinces separately
        l3 = data.rename(columns={"province": "match_string"})
        l2 = data.rename(columns={"region": "match_string"})
        l2["match_string"] = l2["match_string"].apply(lambda x: x.split(": ")[-1])

        # Ensure matching by flagging whether a record must be L2 or L3
        l3["subregion2_code"] = ""
        l2["subregion2_code"] = None
        l3["locality_code"] = None
        l2["locality_code"] = None

        data = concat([l2, l3]).dropna(subset=["match_string"])
        data["country_code"] = "PH"

        # Remove bogus records
        data = data[data["match_string"].notna()]
        data = data[data["match_string"] != ""]
        data = data[data["match_string"] != "REPATRIATE"]
        data = data[data["match_string"] != "CITY OF ISABELA (NOT A PROVINCE)"]
        data = data[data["match_string"] != "COTABATO CITY (NOT A PROVINCE)"]

        return concat([country, data])
