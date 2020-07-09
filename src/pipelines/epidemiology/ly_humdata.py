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
import math
from pandas import DataFrame
from lib.data_source import DataSource
from lib.time import datetime_isoformat
import  datetime

class LibyaHumdataDataSource(DataSource):
   
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Rename the appropriate columns
        data = (
            dataframes[0]
            .rename(
                columns={
                    "Location": "subregion1_name",
                    "Confirmed Cases": "total_confirmed",
                    "Deaths": "total_deceased",
                    "Recoveries": "total_recovered",
                    "Date": "date",
                }
            )
            .drop(columns=["Active"])
            .drop(columns=["total_deceased"])
        )

        # The first row is metadata info about column names - discard it
        data = data[data.subregion1_name != '#loc+name']

        # Convert date to ISO format
        data["date"] = data["date"].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d").strftime('%d-%m-%Y'))
        
        # Convert string numbers to int
        data["total_confirmed"] = data["total_confirmed"].apply(lambda x: 0 if(math.isnan(float(x))) else  int(x))
        data["total_recovered"] = data["total_recovered"].apply(lambda x: 0 if(math.isnan(float(x))) else  int(x))
        
        # Make sure all records have the country code
        data["country_code"] = "LY"

        # Output the results
        return data
