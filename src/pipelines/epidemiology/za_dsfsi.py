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

from typing import Any, Dict, List
from pandas import DataFrame, concat, merge
from lib.pipeline import DataSource
from lib.time import datetime_isoformat
from lib.utils import table_rename, pivot_table, table_multimerge


class Covid19ZaCumulativeDataSource(DataSource):
    @staticmethod
    def _parse_variable(data: DataFrame, var_name: str) -> DataFrame:
        data = data.drop(columns=["YYYYMMDD", "UNKNOWN", "source"])
        return pivot_table(
            data.set_index("date"), pivot_name="subregion1_code", value_name=var_name
        )

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = table_multimerge(
            [
                Covid19ZaCumulativeDataSource._parse_variable(df, name)
                for df, name in zip(
                    dataframes,
                    ["total_confirmed", "total_deceased", "total_recovered", "total_tested"],
                )
            ],
            how="outer",
        )

        # Convert date to ISO format
        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%d-%m-%Y"))

        # Country-level records should have "total" region name
        country_mask = data["subregion1_code"] == "total"
        data.loc[country_mask, "key"] = "ZA"

        # All other records can provide their own key directly
        data.loc[~country_mask, "key"] = "ZA_" + data.subregion1_code

        # Output the results
        return data


class Covid19ZaTimelineTestingDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = table_rename(
            dataframes[0],
            {
                "cumulative_tests": "total_tested",
                "recovered": "total_recovered",
                "hospitalisation": "total_hospitalized",
                "critical_icu": "total_intensive_care",
                "ventilation": "total_ventilator",
                "deaths": "total_deceased",
            },
        )

        # Convert date to ISO format
        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%d-%m-%Y"))

        data["key"] = "ZA"
        return data


class Covid19ZaTimelineDeathsDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        records = []
        sex_buckets = {"gender_male": "male", "gender_female": "female"}
        age_buckets = {
            col: col.replace("age_", "") for col in dataframes[0].columns if col.startswith("age_")
        }
        for _, row in dataframes[0].iterrows():
            for age_col, age_bucket in age_buckets.items():
                records.append(
                    {
                        "key": "ZA",
                        "date": datetime_isoformat(row.date, "%d-%m-%Y"),
                        "age": age_bucket,
                        "total_deceased": row[age_col],
                    }
                )
            for sex_col, sex_bucket in sex_buckets.items():
                records.append(
                    {
                        "key": "ZA",
                        "date": datetime_isoformat(row.date, "%d-%m-%Y"),
                        "sex": sex_bucket,
                        "total_deceased": row[sex_col],
                    }
                )

        return DataFrame.from_records(records)
