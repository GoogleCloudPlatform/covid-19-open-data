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

from lib.data_source import DataSource
from lib.utils import table_rename
from lib.cast import safe_int_cast, numeric_code_as_string
from lib.time import datetime_isoformat


class MadridDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = table_rename(
            dataframes[0],
            {
                "fecha_informe": "date",
                "municipio_distrito": "subregion2_name",
                "codigo_geometria": "subregion2_code",
                "casos_confirmados_totales": "total_confirmed",
            },
            drop=True,
        )

        # Use placeholder code for unknown values
        data.loc[data["subregion2_code"].isna(), "subregion2_code"] = "000000"

        # Region codes need cleaning up to match INEI codes
        data["subregion2_code"] = data["subregion2_code"].apply(
            lambda x: numeric_code_as_string(x, 6)
        )
        data["subregion2_code"] = data["subregion2_code"].apply(
            lambda x: "28" + ("079" + x[4:] if x.startswith("079") else x[2:5] + x[6:])
        )

        data["key"] = "ES_MD_" + data["subregion2_code"]
        data = data.drop(columns=["subregion2_code"])

        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x[:10], "%Y/%m/%d"))
        data["total_confirmed"] = data["total_confirmed"].apply(safe_int_cast)

        # Aggregate the entire autonomous community
        l1 = data.drop(columns=["key", "subregion2_name"]).groupby("date").sum().reset_index()
        l1["key"] = "ES_MD"

        # Sometimes the subregion code is not properly formatted, so we may need to do string match
        data["country_code"] = "ES"
        data["subregion1_code"] = "MD"
        data["subregion2_name"] = data["subregion2_name"].str.replace("Madrid-", "")
        data.loc[data["key"] == "ES_MD_28000", "key"] = None

        return concat([data, l1])
