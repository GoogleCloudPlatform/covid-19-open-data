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
from pathlib import Path
from typing import Any, Dict, Iterable, List
from pandas import DataFrame, concat
from lib.cast import numeric_code_as_string
from lib.concurrent import process_map
from lib.constants import SRC
from lib.io import open_file_like, pbar, read_table
from lib.pipeline import DataSource
from lib.utils import table_rename


def recover_from_rolling_mean(moving_avg: Iterable[float], window_size: int) -> List[float]:
    """ Handy function to get daily data from a rolling average, written by Anton """
    recovered_data = [0] * (window_size - 1)
    for value in moving_avg:
        recovered_data.append(round(window_size * value - sum(recovered_data[1 - window_size :])))
    return recovered_data[window_size - 1 :]


def _process_state(data: DataFrame) -> DataFrame:
    data["date"] = data["date"].apply(lambda x: str(x)[:10])
    data["subregion2_code"] = data["fips_code"].apply(lambda x: numeric_code_as_string(x, 5))
    data["key"] = "US_" + data["state"] + "_" + data["subregion2_code"]
    data.drop(
        columns=[
            "subregion2_code",
            "state",
            "fips_code",
            "county",
            "report_date_window",
            "report_date_window_start",
        ],
        inplace=True,
    )

    # Make sure the data is properly sorted, since we need to compute diffs
    data.sort_values(["key", "date"], inplace=True)

    # Get a mapping between rolling average column names and their daily counterparts
    rolling_suffix = "_7_day_rolling_average"
    rolling_columns_map = {
        col: col.replace(rolling_suffix, "") for col in data.columns if col.endswith(rolling_suffix)
    }

    # Seed the daily versions of the columns with empty values
    for name in rolling_columns_map.values():
        data[name] = None

    # Convert the rolling average columns to daily values one key at a time
    # This can probably be done with some clever grouping function instead, but iteratively is
    # fast enough and it works reliably.
    for key in pbar(data["key"].unique(), desc="Computing daily values from rolling means"):
        mask = data["key"] == key
        for col, name in rolling_columns_map.items():
            data.loc[mask, name] = recover_from_rolling_mean(data.loc[mask, col], 7)

    # Get rid of unnecessary columns now that we have the daily values
    data.drop(columns=rolling_columns_map.keys(), inplace=True)

    return table_rename(
        data,
        {
            "new_cases": "new_confirmed",
            "new_deaths": "new_deceased",
            "new_test_results_reported": "new_tested",
        },
    )


class CDCDataSource(DataSource):
    def fetch(
        self,
        output_folder: Path,
        cache: Dict[str, str],
        fetch_opts: List[Dict[str, Any]],
        skip_existing: bool = False,
    ) -> Dict[str, str]:
        # The URL is just a template which we'll use to download each state
        base_opts = dict(fetch_opts[0])
        url_tpl = base_opts.pop("url")

        # Some states cannot be found in the dataset
        states_banlist = ["AS", "GU", "MP", "PR", "VI"]

        states = read_table(SRC / "data" / "metadata.csv")
        states = states.loc[states["country_code"] == "US", "subregion1_code"].dropna().unique()
        states = [state for state in states if state not in states_banlist]
        opts = [dict(**base_opts, name=code, url=url_tpl.format(state=code)) for code in states]
        return super().fetch(
            output_folder=output_folder, cache=cache, fetch_opts=opts, skip_existing=skip_existing
        )

    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        dataframes = {}
        for name, fname in sources.items():
            with open_file_like(fname, "r") as fd:
                data = json.load(fd)["integrated_county_timeseries_external_data"]
                dataframes[name] = DataFrame.from_records(data)

        return self.parse_dataframes(dataframes, aux=aux, **parse_opts)

    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        # Keep only dataframes which have data available in metadata
        keys = aux["metadata"]["key"]
        has_state = lambda state: keys.apply(lambda x: x.startswith(f"US_{state}")).any()
        dataframes = {state: df for state, df in dataframes.items() if has_state(state)}

        # Parallelize the work and process each state in a different process to speed up the work
        map_opts = dict(total=len(dataframes), desc="Processing states")
        return concat(process_map(_process_state, dataframes.values(), **map_opts))
