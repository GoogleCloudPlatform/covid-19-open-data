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
from typing import Dict, List, Tuple
from pandas import DataFrame
from lib.pipeline import DataSource


class Covid19BrazilTimeseriesDataSource(DataSource):
    def _parse_dataframes(self, dataframes: Tuple[DataFrame, DataFrame], prefix: str):

        # Read data from GitHub repo
        confirmed, deaths = dataframes
        for df in (confirmed, deaths):
            df.rename(columns={"Unnamed: 1": "subregion1_code"}, inplace=True)
            df.set_index("subregion1_code", inplace=True)

        # Transform the data from non-tabulated format to record format
        records = []
        for region_code in confirmed.index.unique():
            if "(" in region_code or region_code == "BR":
                continue
            for col in confirmed.columns[1:]:
                date = col + "/" + str(datetime.now().year)
                date = datetime.strptime(date, "%d/%m/%Y").date().isoformat()
                records.append(
                    {
                        "date": date,
                        "country_code": "BR",
                        "subregion1_code": region_code,
                        prefix + "confirmed": confirmed.loc[region_code, col],
                        prefix + "deceased": deaths.loc[region_code, col],
                    }
                )

        return DataFrame.from_records(records)

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Parse cumsum and daily separately
        data_cum = self._parse_dataframes((dataframes[0], dataframes[1]), "total_").set_index(
            ["date", "country_code", "subregion1_code"]
        )
        data_new = self._parse_dataframes((dataframes[2], dataframes[3]), "new_").set_index(
            ["date", "country_code", "subregion1_code"]
        )
        return data_new.join(data_cum).reset_index()
