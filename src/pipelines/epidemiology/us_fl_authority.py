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

import uuid
import datetime
from pathlib import Path
from typing import Any, Dict, List

import requests
from pandas import DataFrame
from pandas.api.types import is_numeric_dtype

from lib.cast import age_group, safe_int_cast
from lib.pipeline import DataSource
from lib.utils import table_rename

fromtimestamp = datetime.datetime.fromtimestamp

_url_tpl = (
    "https://services1.arcgis.com/CY1LXxl9zlJeBuRZ/ArcGIS/rest/services"
    "/Florida_COVID19_Case_Line_Data_NEW/FeatureServer/0/query"
    "?where=1%3D1&outFields=*&resultOffset={offset}&f=json"
)


def _convert_cases_to_time_series(cases: DataFrame, index_columns: List[str] = None):
    """
    Converts a DataFrame of line (individual case) data into time-series data. The input format
    is expected to have the columns:
        - key
        - age
        - sex
        - date_${statistic}

    The output will be our familiar time-series format:
        - key
        - date
        - sex (bin)
        - age (bin)
        - ${statistic}
    """
    index_columns = ["key", "date", "sex", "age"] if index_columns is None else index_columns

    # Create stratified age bands
    if is_numeric_dtype(cases.age):
        cases.age = cases.age.apply(age_group)

    # Rename the sex values and drop unknown labels
    cases.sex = cases.sex.str.lower().apply(lambda x: {"m": "male", "f": "female"}.get(x, x))
    cases = cases[cases.sex.isin({"male", "female"})]

    # Go from individual case records to key-grouped records in a flat table
    merged: DataFrame = None
    for value_column in [col.split("date_")[-1] for col in cases.columns if "date_" in col]:
        subset = cases.rename(columns={"date_{}".format(value_column): "date"})[index_columns]
        subset = subset[~subset.date.isna()].dropna()
        subset[value_column] = 1
        subset = subset.groupby(index_columns).sum().reset_index()
        if merged is None:
            merged = subset
        else:
            merged = merged.merge(subset, how="outer")

    # We can fill all missing records as zero since we know we have "perfect" information
    return merged.fillna(0)


class FloridaDataSource(DataSource):
    def _get_county_cases(self) -> DataFrame:
        def _r_get_county_cases(offset: int = 0) -> List[Dict[str, str]]:
            try:
                res = requests.get(_url_tpl.format(offset=offset)).json()["features"]
            except Exception as exc:
                self.errlog(requests.get(_url_tpl.format(offset=offset)).text)
                raise exc
            rows = [row["attributes"] for row in res]
            if len(rows) == 0:
                return rows
            else:
                return rows + _r_get_county_cases(offset + len(rows))

        cases = DataFrame.from_records(_r_get_county_cases())
        cases["date_new_confirmed"] = cases.ChartDate.apply(
            lambda x: fromtimestamp(x // 1000).date().isoformat()
        )

        # FL does not provide date for deceased or hospitalized, so we just copy it from confirmed
        deceased_mask = cases.Died == "Yes"
        hospitalized_mask = cases.Hospitalized == "YES"
        cases["date_new_deceased"] = None
        cases["date_new_hospitalized"] = None
        cases.loc[deceased_mask, "date_new_deceased"] = cases.loc[
            deceased_mask, "date_new_confirmed"
        ]
        cases.loc[hospitalized_mask, "date_new_hospitalized"] = cases.loc[
            hospitalized_mask, "date_new_confirmed"
        ]

        # Rename the sex labels
        cases["sex"] = cases.Gender.str.lower()

        # Rename columns and return
        return table_rename(
            cases, {"County": "match_string", "Age": "age", "Sex": "sex"}, remove_regex=r"[^a-z\s_]"
        )

    def fetch(
        self, output_folder: Path, cache: Dict[str, str], fetch_opts: List[Dict[str, Any]]
    ) -> Dict[str, str]:

        # Create a deterministic file name
        file_path = (
            output_folder
            / "snapshot"
            / ("%s.%s" % (uuid.uuid5(uuid.NAMESPACE_DNS, _url_tpl), "csv"))
        )

        # Avoid download if the file exists and flag is set
        skip_existing = (fetch_opts or [{}])[0].get("opts", {}).get("skip_existing")
        if not skip_existing or not file_path.exists():
            self._get_county_cases().to_csv(file_path, index=False)

        return {0: str(file_path.absolute())}

    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        cases = dataframes[0]
        cases.age = cases.age.apply(safe_int_cast)
        data = _convert_cases_to_time_series(
            cases, index_columns=["match_string", "date", "age", "sex"]
        )
        data["country_code"] = "US"
        data["subregion1_code"] = "FL"

        # Remove bogus data
        data = data[data.match_string != "Unknown"]

        # Death and hospitalization data do not have accurate dates, so we provide an option to
        # remove them since for some pipelines we can source the data elsewhere
        if parse_opts.get("remove_inaccurate_statistics"):
            data.drop(columns=["new_deceased", "new_hospitalized"], inplace=True)

        return data
