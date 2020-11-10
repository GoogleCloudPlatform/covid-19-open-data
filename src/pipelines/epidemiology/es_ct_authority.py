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


class CataloniaMunicipalitiesDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = table_rename(
            dataframes[0],
            {
                "TipusCasData": "date",
                # "ComarcaCodi": "comarca_code",
                # "ComarcaDescripcio": "comarca_name",
                "MunicipiCodi": "subregion2_code",
                "MunicipiDescripcio": "subregion2_name",
                "SexeCodi": "sex",
                # "SexeDescripcio": "sex",
                "TipusCasDescripcio": "_case_type",
                "NumCasos": "new_confirmed",
            },
            drop=True,
        )

        # Remove "suspect" cases
        data = data[data["_case_type"] != "Sospitós"].drop(columns=["_case_type"])

        # Use placeholder code for unknown values
        data.loc[data["subregion2_code"].isna(), "subregion2_code"] = "00000"

        # Region codes need cleaning up to match INEI codes
        data["subregion2_code"] = data["subregion2_code"].apply(
            lambda x: numeric_code_as_string(x, 5)
        )

        # Derive key from subregion code
        data["key"] = "ES_CT_" + data["subregion2_code"]

        # Parse sex, date and numeric values
        sex_adapter = {"0": "male", "1": "female"}
        data["sex"] = data["sex"].apply(lambda x: sex_adapter.get(x, "sex_unknown"))
        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%d/%m/%Y"))
        data["new_confirmed"] = data["new_confirmed"].apply(safe_int_cast)

        # Aggregate manually since some municipalities are clumped together if they are too small
        ccaa = data.drop(columns=["subregion2_code"]).groupby(["date", "sex"]).sum().reset_index()
        ccaa["key"] = "ES_CT"

        # Remove unnecessary data
        data = data[data["key"] != "ES_CT_00000"]

        return concat([ccaa, data])


class CataloniaHealthDeptDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = table_rename(
            dataframes[0],
            {
                "TipusCasData": "date",
                "SexeCodi": "sex",
                # "SexeDescripcio": "sex",
                "EdatRang": "age",
                "TipusCasDescripcio": "_case_type",
                "NumCasos": "new_confirmed",
            },
            drop=True,
        )

        # Remove "suspect" cases
        data = data[data["_case_type"] != "Sospitós"].drop(columns=["_case_type"])

        # Derive key from subregion code
        data["key"] = "ES_CT"

        # Parse age, sex, date and numeric values
        sex_adapter = {"0": "male", "1": "female"}
        data["age"] = data["age"].str.replace("90\+", "90-")
        data["sex"] = data["sex"].apply(lambda x: sex_adapter.get(x, "sex_unknown"))
        data["date"] = data["date"].apply(lambda x: datetime_isoformat(x, "%d/%m/%Y"))
        data["new_confirmed"] = data["new_confirmed"].apply(safe_int_cast)

        return data
