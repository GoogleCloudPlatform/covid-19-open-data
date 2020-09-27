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

import re
from typing import Any, Callable, Dict, List
from pandas import DataFrame
from unidecode import unidecode
from lib.cast import age_group, isna, safe_int_cast
from lib.constants import SRC
from lib.io import read_file
from lib.utils import get_or_default

STRATIFIED_VALUES = read_file(SRC / "data" / "stratified_values.csv").set_index("type")


def _default_adapter_factory(key: str) -> Callable[[str], str]:

    mapping = {"other": f"{key}_other", "unknown": f"{key}_unknown"}
    for value, alias in STRATIFIED_VALUES.loc[key].set_index("value")["alias"].iteritems():
        mapping[value] = value
        if not isna(alias):
            mapping[alias] = value

    def default_adapter(value: str):
        if isna(value):
            return mapping["unknown"]
        value = re.sub(r"[\s\-]", "_", unidecode(str(value).lower()))
        return get_or_default(mapping, value, mapping["unknown"])

    return default_adapter


def _default_age_adapter(value: Any) -> str:
    if isna(value):
        return "age_unknown"

    try:
        value_int = safe_int_cast(value)
        if value_int is not None:
            return age_group(value_int)
        if re.match(r"\d+\-\d*", value):
            return value
    except ValueError:
        pass

    return "age_unknown"


DEFAULT_BIN_ADAPTERS = {
    "age": _default_age_adapter,
    "sex": _default_adapter_factory("sex"),
    "ethnicity": _default_adapter_factory("ethnicity"),
}


def convert_cases_to_time_series(
    cases: DataFrame,
    index_columns: List = None,
    bin_adapters: Dict[str, Callable[[Any], str]] = None,
) -> DataFrame:
    """
    Converts a DataFrame of line (individual case) data into time-series data. The input format
    is expected to have the columns:
        - key
        - [age]
        - [sex]
        - [ethnicity]
        - date_${statistic}

    The output will be our familiar time-series format:
        - key
        - date
        - sex (aggregated by bucket)
        - age (aggregated by bucket)
        - ethnicity (aggregated by bucket)
        - ${statistic}

    Arguments:
        cases: DataFrame in the case-line format, see above for details
        index_columns: Columns which will be used for grouping regardless of buckets
        bin_adapters: Map of <column name, adapter> where the adapter takes a case value as input
            outputs a bucket value, for example age adapter `(3) -> "0-9"`
    Returns:
        DataFrame: time-series formatted data table
    """
    index_columns = ["key"] if index_columns is None else index_columns

    assert all(
        col in cases.columns for col in index_columns
    ), f"Expected all columns {index_columns} to be in {cases.columns}"

    string_columns = cases.select_dtypes(include="object")
    assert all(
        col in string_columns for col in index_columns
    ), f"Expected for all {index_columns} to be of type string"

    # Fill in the bin adapters with default implementations
    bin_adapters = {**(bin_adapters or {}), **DEFAULT_BIN_ADAPTERS}
    bin_adapters = {col: adapter for col, adapter in bin_adapters.items() if col in cases.columns}

    # Remove all columns which are not indexable
    index_columns = ["date"] + index_columns + list(bin_adapters.keys())
    cases = cases[[col for col in cases.columns if col in index_columns or col.startswith("date_")]]
    cases = cases.copy()

    # Apply the bin adapters to all of the known, allowed bucket types
    for col, adapter in bin_adapters.items():
        cases[col] = cases[col].apply(adapter)

    # Go from individual case records to key-grouped records in a flat table
    merged: DataFrame = None
    for value_column in [col.split("date_")[-1] for col in cases.columns if "date_" in col]:
        subset = cases.rename(columns={"date_{}".format(value_column): "date"})[index_columns]
        # Index columns are all expected to be of str type so we replace NaN with empty string
        # Replacement necessary to work around https://github.com/pandas-dev/pandas/issues/3729
        subset = subset.dropna(subset=["date"]).fillna("")
        subset[value_column] = 1
        subset = subset.groupby(index_columns).sum().reset_index()
        if merged is None:
            merged = subset
        else:
            merged = merged.merge(subset, how="outer")

    # We can fill all missing records as zero since we know we have "perfect" information
    return merged.fillna(0)
