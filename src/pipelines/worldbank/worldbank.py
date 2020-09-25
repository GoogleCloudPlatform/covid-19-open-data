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

import traceback
import zipfile
from io import BytesIO
from pathlib import Path
from functools import partial
from typing import Any, Dict, List

from pandas import DataFrame, Series, isna, read_csv

from lib.concurrent import thread_map
from lib.data_source import DataSource
from lib.net import download


class WorldbankDataSource(DataSource):
    """ Retrieves the requested properties from Wikidata for all items in metadata.csv """

    def _get_latest(self, record: Series, min_year: int) -> Any:
        # Only look at data starting on <min_year>, if none found return null
        try:
            for year in [str(i) for i in range(min_year, 2020)][::-1]:
                if year in record and not isna(record[year]):
                    return record[year]
        except Exception as exc:
            self.log_error(str(exc), traceback=traceback.format_exc(), record=record)
        return None

    def fetch(
        self, output_folder: Path, cache: Dict[str, str], fetch_opts: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        # Data file is too big to store in Git, so pass-through the URL to parse manually
        return {idx: source["url"] for idx, source in enumerate(fetch_opts)}

    def _process_record(
        self, worldbank: DataFrame, indicators: Dict[str, str], min_year: int, key: str
    ):
        record = {"key": key}
        subset = worldbank[key]
        indicators = {name: code for name, code in indicators.items() if code in subset.index}
        for name, code in indicators.items():
            row = subset.loc[code:code].iloc[0]
            record[name] = self._get_latest(row, min_year)
        return record

    def parse(self, sources: List[str], aux: Dict[str, DataFrame], **parse_opts):

        buffer = BytesIO()
        download(sources[0], buffer, progress=True)

        data = None
        with zipfile.ZipFile(buffer) as zipped:
            data = zipped.read("WDIData.csv")
            data = read_csv(BytesIO(data))
        assert data is not None

        data = data.rename(
            columns={
                "Country Code": "3166-1-alpha-3",
                "Indicator Name": "indicator_name",
                "Indicator Code": "indicator_code",
            }
        )

        data = data.merge(aux["worldbank_indicators"]).merge(aux["country_codes"])
        data = data.drop(columns=["Country Name", "3166-1-alpha-2", "3166-1-alpha-3"])

        indicators = parse_opts.get(
            "indicators", {code: code for code in data.indicator_code.values}
        )
        min_year = int(parse_opts.get("min_year", 2015))
        data = data[data.indicator_code.isin(indicators.values())]

        # Index data by indicator code for performance optimization
        keys = data.key.unique()
        indexed = {key: data[data.key == key].set_index("indicator_code") for key in keys}

        # There is probably a fancy pandas function to this more efficiently but this works for now
        map_func = partial(self._process_record, indexed, indicators, min_year)
        records = thread_map(map_func, keys, desc="WorldBank Indicators")

        # Some countries are better described as subregions
        data = DataFrame.from_records(records)
        data.loc[data.key == "MF", "key"] = "FR_MF"

        # Return all records in DataFrame form
        return data
