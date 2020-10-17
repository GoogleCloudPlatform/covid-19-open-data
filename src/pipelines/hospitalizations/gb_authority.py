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
from lib.cast import safe_float_cast
from lib.io import read_file
from lib.data_source import DataSource
from lib.utils import pivot_table, table_rename
from uk_covid19 import Cov19API


class ScotlandDataSource(DataSource):
    @staticmethod
    def _parse(file_path: str, sheet_name: str, value_name: str):
        data = read_file(file_path, sheet_name=sheet_name)

        data.columns = [col.replace("NHS ", "").replace(" total", "") for col in data.iloc[1]]
        # Drop Golden Jubilee National Hospital - it has no hospitalizations and does not fit
        # any current matches in metadata.csv.
        data = data.drop(columns=["Golden Jubilee National Hospital"])
        data = data.iloc[2:].rename(columns={"Date": "date"})

        data = pivot_table(data.set_index("date"), pivot_name="match_string")
        data = data.rename(columns={"value": value_name})
        data[value_name] = data[value_name].replace("*", None).apply(safe_float_cast).astype(float)

        # Get date in ISO format
        data.date = data.date.apply(lambda x: x.date().isoformat())

        # Add metadata
        data["key"] = None
        data["country_code"] = "GB"
        data["subregion1_code"] = "SCT"
        l2_mask = data.match_string == "Scotland"
        data.loc[l2_mask, "key"] = "GB_SCT"

        return data

    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        hospitalized = ScotlandDataSource._parse(
            sources[0], sheet_name="Table 3a - Hospital Confirmed", value_name="new_hospitalized"
        )
        intensive_care = ScotlandDataSource._parse(
            sources[0], sheet_name="Table 2a - ICU patients", value_name="new_intensive_care"
        )

        return hospitalized.merge(intensive_care, how="outer")


class UKL1DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Specify filter for overview / consolidated data
        # for the UK
        api_filter_overview = ["areaType=overview"]

        # Specify relevant metrics that will be used
        # according to Google's schema
        api_structure_hospitalization = {
            "date": "date",
            "newAdmissions": "newAdmissions",
            "cumAdmissions": "cumAdmissions",
            "hospitalCases": "hospitalCases",
            "covidOccupiedMVBeds": "covidOccupiedMVBeds",
        }

        api = Cov19API(filters=api_filter_overview, structure=api_structure_hospitalization)

        data = api.get_dataframe()

        # Rename columns and map to expected schema
        data = table_rename(
            data,
            {
                "date": "date",
                "newAdmissions": "new_hospitalized",
                "cumAdmissions": "total_hospitalized",
                "hospitalCases": "current_hospitalized",
                "covidOccupiedMVBeds": "current_ventilator",
            },
            drop=True,
        )

        # Add key
        data["key"] = "GB"

        return data
