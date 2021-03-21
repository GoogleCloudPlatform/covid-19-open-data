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

from pathlib import Path
from typing import Any, Dict, List

import requests
from pandas import DataFrame, concat, melt

from lib.case_line import convert_cases_to_time_series
from lib.cast import safe_int_cast
from lib.data_source import DataSource
from lib.net import download_snapshot
from lib.time import datetime_isoformat
from lib.utils import aggregate_admin_level, table_rename


class Covid19IndiaOrgL1DataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        data = dataframes[0]
        # Get all the states
        states = list(data.columns.difference(["Status", "Date"]))
        # Flatten the table
        data = melt(data, id_vars=["Date", "Status"], value_vars=states, var_name="subregion1_code")
        # Convert numeric fields to integers
        data["value"] = data["value"].apply(safe_int_cast)
        # Pivot on Status to get flattened confirmed, deceased, recovered numbers
        data = data.pivot_table("value", ["Date", "subregion1_code"], "Status")
        data.reset_index(drop=False, inplace=True)
        data = data.reindex(
            ["Date", "subregion1_code", "Confirmed", "Deceased", "Recovered"], axis=1
        )

        data = data.rename(
            columns={
                "Confirmed": "new_confirmed",
                "Deceased": "new_deceased",
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
class Covid19IndiaOrgL2DataSource(DataSource):
    """ Add L3 data for India districts. """

    def _replace_subregion(self, x):
        if x in L3_INDIA_REPLACEMENTS:
            return L3_INDIA_REPLACEMENTS[x]
        return x

    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = dataframes[0]

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


class Covid19IndiaOrgCasesDataSource(DataSource):
    def fetch(
        self,
        output_folder: Path,
        cache: Dict[str, str],
        fetch_opts: List[Dict[str, Any]],
        skip_existing: bool = False,
    ) -> Dict[str, str]:
        output = {}
        curr_idx = 1
        url_tpl = fetch_opts[0].get("url")
        download_options = dict(fetch_opts[0].get("opts", {}), skip_existing=skip_existing)
        while True:
            try:
                url = url_tpl.format(idx=curr_idx)
                fname = download_snapshot(url, output_folder, **download_options)
                output.update({curr_idx: fname})
                curr_idx += 1
            except requests.HTTPError:
                break

        assert len(output) > 0, "No data downloaded"
        return output

    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        cases = table_rename(
            concat(dataframes.values()),
            {
                # "Patient Number": "",
                # "State Patient Number": "",
                "Date Announced": "date_new_confirmed",
                # "Estimated Onset Date": "",
                "Age Bracket": "age",
                "Gender": "sex",
                # "Detected City": "",
                "Detected District": "subregion2_name",
                "Detected State": "subregion1_name",
                # "State code": "subregion1_code",
                "Current Status": "_prognosis",
                # "Notes": "",
                # "Contracted from which Patient (Suspected)": "",
                # "Nationality": "",
                # "Type of transmission": "",
                "Status Change Date": "_change_date",
                # "Source_1": "",
                # "Source_2": "",
                # "Source_3": "",
                # "Backup Notes": "",
                "Num Cases": "new_confirmed",
                "Entry_ID": "",
            },
            drop=True,
        )

        # Convert dates to ISO format
        for col in [col for col in cases.columns if "date" in col]:
            cases[col] = cases[col].apply(lambda x: datetime_isoformat(x, "%d/%m/%Y"))

        cases["age"] = cases["age"].astype(str)
        cases["age"] = cases["age"].str.lower()
        cases["age"] = cases["age"].str.replace("\.0", "")
        cases["age"] = cases["age"].str.replace(r"[\d\.]+ day(s)?", "1")
        cases["age"] = cases["age"].str.replace(r"[\d\.]+ month(s)?", "1")
        cases.loc[cases["age"].str.contains("-"), "age"] = None

        sex_adapter = lambda x: {"M": "male", "F": "female"}.get(x, "sex_unknown")
        cases["sex"] = cases["sex"].str.strip()
        cases["sex"] = cases["sex"].apply(sex_adapter)

        cases["date_new_deceased"] = None
        deceased_mask = cases["_prognosis"] == "Deceased"
        cases.loc[deceased_mask, "date_new_deceased"] = cases.loc[deceased_mask, "_change_date"]

        cases["date_new_hospitalized"] = None
        hosp_mask = cases["_prognosis"] == "Hospitalized"
        cases.loc[hosp_mask, "date_new_hospitalized"] = cases.loc[hosp_mask, "_change_date"]

        data = convert_cases_to_time_series(cases, ["subregion1_name", "subregion2_name"])
        data["country_code"] = "IN"

        # Aggregate country level and admin level 1
        country = aggregate_admin_level(data, ["date", "age", "sex"], "country")
        subregion1 = aggregate_admin_level(data, ["date", "age", "sex"], "subregion1")
        subregion1 = subregion1[subregion1["subregion1_name"].str.lower() != "state unassigned"]

        # Data for admin level 2 is too noisy and there are many mismatches, so we only return
        # the aggregated country level and admin level 1 data
        return concat([country, subregion1])
