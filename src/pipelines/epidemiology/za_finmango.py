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

import datetime
from typing import Any, Dict, List
from pandas import DataFrame, concat
from lib.cast import safe_int_cast
from lib.constants import SRC
from lib.data_source import DataSource
from lib.io import read_file
from pathlib import Path

_columns = [
    "date",
    "name",
    "code",
    "new_confirmed",
    "current_confirmed",
    "total_confirmed",
    "new_deceased",
    "current_deceased",
    "total_deceased",
    "new_tested",
    "current_tested",
    "total_tested",
    "new_hospitalized",
    "current_hospitalized",
    "total_hospitalized",
    "new_intensive_care",
    "current_intensive_care",
    "total_intensive_care",
    "new_ventilator",
    "current_ventilator",
    "total_ventilator",
    "_source_link",
]


class FinMangoSheetsDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        # Data is nested into multiple sheets
        tables = []
        for df in dataframes[0].values():
            df.columns = _columns
            tables.append(df.iloc[2:])

        # Put all sheets together into a single DataFrame
        data = concat(tables)
        data["key"] = parse_opts["country"] + "_" + data["code"].str.replace("-", "_")

        # Ensure date is in ISO format
        data["date"] = data["date"].apply(lambda x: str(x)[:10])

        # Make sure that all data is numeric
        for col in data.columns:
            if col not in ("date", "name", "key", "code", "_source_link"):
                data[col] = data[col].apply(safe_int_cast)

        # Remove the "new" columns since cumulative data is more reliable
        data = data.drop(columns=["new_confirmed", "new_deceased"])

        # Output the results
        return data


class FinMangoGithubDataSource(DataSource):
    def fetch(
        self,
        output_folder: Path,
        cache: Dict[str, str],
        fetch_opts: List[Dict[str, Any]],
        skip_existing: bool = False,
    ) -> Dict[str, str]:
        metadata = read_file(SRC / "data" / "metadata.csv")
        za = metadata[metadata["country_code"] == "ZA"]
        provinces = za[za["key"].apply(lambda x: len(x.split("_")) == 2)]
        districts = za[za["key"].apply(lambda x: len(x.split("_")) == 3)]
        url_tpl = {opt["name"]: opt["url"] for opt in fetch_opts}
        opts = {"opts": {"ignore_failure": True}}

        fetch_list = []
        for key in provinces["key"]:
            key_ = key[3:].replace("_", "-")
            fetch_list.append({"url": url_tpl["provinces"].format(key=key_), "name": key, **opts})
        for key in districts["key"]:
            key_ = key[3:].replace("_", "-")
            fetch_list.append({"url": url_tpl["districts"].format(key=key_), "name": key, **opts})

        return super().fetch(output_folder, cache, fetch_list, skip_existing=skip_existing)

    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = None
        for key, df in dataframes.items():
            df["key"] = key
            if data is None:
                data = df
            else:
                data = data.append(df)

        data["date"] = data["ID"].apply(lambda x: datetime.datetime.fromtimestamp(x / 1_000))
        data["date"] = data["date"].apply(lambda x: x.date().isoformat())
        data = data.rename(columns={"Cases": "total_confirmed"}).drop(columns=["Date", "ID"])
        return data
