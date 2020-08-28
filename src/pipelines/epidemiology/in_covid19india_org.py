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
from pandas import DataFrame, melt
from lib.data_source import DataSource
from lib.time import datetime_isoformat
from lib.utils import table_rename
import datetime


class Covid19IndiaOrgL2DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = dataframes[0]
        # Get all the states
        states = list(data.columns.difference(["Status", "Date"]))
        # Flatten the table
        data = melt(data, id_vars=["Date", "Status"], value_vars=states, var_name="subregion1_code")
        # Pivot on Status to get flattened confirmed, deceased, recovered numbers
        data = data.pivot_table("value", ["Date", "subregion1_code"], "Status")
        data.reset_index(drop=False, inplace=True)
        data = data.reindex(
            ["Date", "subregion1_code", "Confirmed", "Deceased", "Recovered"], axis=1
        )

        data = data.rename(
            columns={
                "Confirmed": "new_confirmed",
                "Deaths": "new_deceased",
                "Recovered": "new_recovered",
                "Date": "date",
            }
        )
        # No data is recorded against IN_DD, it is now a district of IN_DN
        data = data[data.subregion1_code != "DD"]
        data.date = data.date.apply(lambda x: datetime_isoformat(x, "%d-%b-%y"))
        data["key"] = "IN_" + data["subregion1_code"]

        return data


L3_INDIA_REMOVE_SET = set(
    [
        "Delhi",
        "CAPF Personnel",
        "BSF Camp",
        "Airport Quarantine",
        "Evacuees",
        "Foreign Evacuees",
        "Italians",
        "Other Region",
        "Other State",
        "Others",
        "Railway Quarantine",
        "Unknown",
    ]
)
# For some of these mappings both the "correct" metadata version and an incorrect version are used.
# Harmonize both here.
L3_INDIA_REPLACEMENTS = {
    "Upper Dibang Valley": "Dibang",
    "Dibang Valley": "Dibang",
    "Kra-Daadi": "Kra Daadi",
    "Kamrup Metropolitan": "Kamrup",
    "Bametara": "Bemetara",
    "Koriya": "Korea",
    "Gariaband": "Gariyaband",
    "Gaurela Pendra Marwahi": "Gaurella Pendra Marwahi",
    "Janjgir Champa": "Janjgir-Champa",
    "Kabeerdham": "Kabirdham",
    "Uttar Bastar Kanker": "Bastar",
    "Banaskantha": "Banas Kantha",
    "Chhota Udaipur": "Chhotaudepur",
    "Dahod": "Dohad",
    "Kutch": "Kachchh",
    "Mehsana": "Mahesana",
    "Panchmahal": "Panch Mahals",
    "Sabarkantha": "Sabar Kantha",
    "Charkhi Dadri": "Charki Dadri",
    "Lahaul and Spiti": "Lahul and Spiti",
    "Punch": "Poonch",
    "Shopiyan": "Shopian",
    "Saraikela-Kharsawan": "Saraikela Kharsawan",
    "Davanagere": "Davangere",
    "Leh": "Leh Ladakh",
    "Dakshin Bastar Dantewada": "Dantewada",
    "Ribhoi": "Ri Bhoi",
    "Balasore": "Baleshwar",
    "Nabarangapur": "Nabarangpur",
    "Viluppuram": "Villupuram",
    "Sipahijala": "Sepahijala",
    "Unokoti": "Unakoti",
}

# Data for districts in India taken from https://lgdirectory.gov.in/
# The following districts were missing a code, so @themonk911 gave them reasonable
# codes based on the name.
# LEPARADA  ARUNACHAL PRADESH(State)
# PAKKE KESSANG   ARUNACHAL PRADESH(State)
# SHI YOMI    ARUNACHAL PRADESH(State)
# Gaurella Pendra Marwahi CHHATTISGARH(State)
# Hnahthial   MIZORAM(State)
# KHAWZAWL    MIZORAM(State)
# SAITUAL MIZORAM(State)
# CHENGALPATTU    TAMIL NADU(State)
# KALLAKURICHI    TAMIL NADU(State)
# Ranipet TAMIL NADU(State)
# TENKASI TAMIL NADU(State)
# Tirupathur  TAMIL NADU(State)
# Thoothukkudi was missing from Tamil Nadu so was added.
class Covid19IndiaOrgL3DataSource(DataSource):
    """ Add L3 data for India districts. """

    def _replace_subregion(self, x):
        if x in L3_INDIA_REPLACEMENTS:
            return L3_INDIA_REPLACEMENTS[x]
        return x

    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = dataframes[0]
        # Get all the states
        states = list(data.columns.difference(["Status", "Date"]))

        data = table_rename(
            data,
            {
                "Confirmed": "total_confirmed",
                "Deceased": "total_deceased",
                "Recovered": "total_recovered",
                "Tested": "total_tested",
                "Date": "date",
                "District": "match_string",
                "State": "subregion1_name",
            },
            drop=True,
        )
        data.match_string = data.match_string.apply(self._replace_subregion)

        data = data[~data.match_string.isin(L3_INDIA_REMOVE_SET)]

        data["country_code"] = "IN"

        return data
