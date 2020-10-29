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

from datetime import datetime
from typing import Dict
from pandas import DataFrame, concat
from lib.data_source import DataSource


class NetherlandsDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = dataframes[0].rename(
            columns={
                "Date_of_report": "date",
                "Municipality_code": "subregion2_code",
                "Province": "subregion1_name",
                "Total_reported": "total_confirmed",
                "Hospital_admission": "total_hospitalized",
                "Deceased": "total_deceased",
            }
        )

        # Get date in ISO format
        data.date = data.date.apply(lambda x: datetime.fromisoformat(x).date().isoformat())

        # Group by country level, and add the parts
        country = data.copy().drop(columns=["subregion1_name", "subregion2_code"])
        country = country.groupby("date").sum().reset_index()
        country["key"] = "NL"

        # Group by province, and add the parts
        provinces = data.copy().dropna(subset=["subregion2_code"])
        provinces = provinces.groupby(["subregion1_name", "date"]).sum().reset_index()
        provinces = provinces.rename(columns={"subregion1_name": "match_string"})
        provinces["subregion2_code"] = None

        # Drop data without a clear demarcation
        data = data[~data.subregion1_name.isna()]
        data = data[~data.subregion2_code.isna()]

        # Get date in ISO format
        data.date = data.date.apply(lambda x: datetime.fromisoformat(x).date().isoformat())

        # Make sure the region code is zero-padded and without prefix
        data["subregion2_code"] = data["subregion2_code"].apply(lambda x: x[2:])

        # Add the country to help with matching
        metadata = aux["metadata"]
        metadata = metadata[metadata["country_code"] == "NL"]
        data = data.drop(columns=["subregion1_name"])
        data = data.merge(metadata, on=["subregion2_code"])

        # We only need to keep key-date pair for identification
        data = data[["date", "key", "total_confirmed", "total_deceased", "total_hospitalized"]]

        # Output the results
        return concat([country, provinces, data])
