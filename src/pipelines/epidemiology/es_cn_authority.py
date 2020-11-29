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

import json
import uuid
from pathlib import Path
from datetime import datetime
from typing import Any, Callable, Dict, List

import requests
from pandas import DataFrame, concat

from lib.arcgis_data_source import ArcGISDataSource
from lib.utils import table_merge


_island_map = {
    "TENERIFE": "TFN",
    "GRAN CANARIA": "LPA",
    "LANZAROTE": "ACE",
    "LA PALMA": "SPC",
    "LA GOMERA": "GMZ",
    "FUERTEVENTURA": "FUE",
    "EL HIERRO": "VDE",
}


class CanaryIslandsDataSource(ArcGISDataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        with open(sources[0], "r") as fd:
            features = json.load(fd)["features"]

        records = {"confirmed": [], "deceased": [], "recovered": []}
        for record in features:
            if record["TIPO"] == "Casos":
                statistic = "confirmed"
            elif record["TIPO"] == "Fallecidos":
                statistic = "deceased"
            elif record["TIPO"] == "Recuperados":
                statistic = "recovered"
            else:
                self.log_error(f"Unknown statistic type: {statistic}")
                continue
            records[statistic].append(
                {
                    "date": datetime.fromtimestamp(record["FECHA"] / 1000).date().isoformat(),
                    "subregion2_code": record["CODMUN"],
                    "subregion2_name": record["MUNICIPIO"],
                    f"new_{statistic}": record["CV19_DIA"],
                    f"total_{statistic}": record["CV19_AC"],
                    "_island": record["ISLA"],
                }
            )

        dataframes = [DataFrame.from_records(df) for df in records.values()]
        data = table_merge(dataframes, how="outer")
        data["key"] = "ES_CN_" + data["subregion2_code"].astype(str)

        # Add the country and region code to all records
        data["country_code"] = "ES"
        data["subregion1_code"] = "CN"

        # Aggregate by island and map to known key
        islands = (
            data.drop(columns=["key", "subregion2_code", "subregion2_name"])
            .groupby(["date", "_island"])
            .sum()
            .reset_index()
        )
        islands["key"] = "ES_CN_" + islands["_island"].apply(_island_map.get)

        # Aggregate the entire autonomous community
        l1 = islands.drop(columns=["key", "_island"]).groupby("date").sum().reset_index()
        l1["key"] = "ES_CN"

        # Drop bogus values
        data = data[data["subregion2_code"] != 0]
        islands = islands[~islands["key"].isna()]

        return concat([data, islands, l1])
