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

import warnings
from typing import Dict, List
from pandas import DataFrame
from pandas.api.types import is_numeric_dtype
from .cast import safe_float_cast


def _detect_perform_action(msg: str, tags: List[str], action: str):
    msg = "".join([f"[{tag}]" for tag in tags]) + " " + msg
    if action == "warn":
        warnings.warn(msg)
    elif action == "raise":
        raise ValueError(msg)
    else:
        raise TypeError("Unknown action {}".format(action))


def detect_correct_schema(
    schema: Dict[str, type], data: DataFrame, tags: List[str], action: str = "warn"
) -> None:
    missing_columns = set(schema.keys()).difference(data.columns)
    if len(missing_columns) > 0:
        _detect_perform_action(
            "Missing columns from schema: {}".format(missing_columns), tags, action
        )


def detect_null_columns(
    schema: Dict[str, type], data: DataFrame, tags: List[str], action: str = "warn"
) -> None:
    for column in data.columns:
        if sum(~data[column].isnull()) == 0:
            _detect_perform_action("Null column detected: " + column, tags, action)


def detect_zero_columns(
    schema: Dict[str, type], data: DataFrame, tags: List[str], action: str = "warn"
) -> None:
    for column in data.columns:
        if len(data[column]) == 0:
            continue
        if not is_numeric_dtype(data[column]):
            continue
        if sum(~data[column].isnull()) == 0:
            # Already flagged by detect_null_columns
            continue
        if sum(data[column].apply(safe_float_cast).fillna(0).apply(abs)) < 1:
            _detect_perform_action("All-zeroes column detected: " + column, tags, action)


def detect_stale_columns(
    schema: Dict[str, type], data: DataFrame, tags: List[str], action: str = "warn"
) -> None:
    if "date" not in schema:
        return
    last_dates = list(sorted(data.date.unique()))[-3:]
    for column in data.columns:
        subset = data[["date", column]].dropna()
        if len(subset) == 0:
            # Already flagged by detect_null_columns
            continue
        if len(subset[subset.date.isin(last_dates)]) == 0:
            _detect_perform_action("Stale column detected: " + column, tags, action)


def detect_anomaly_all(
    schema: Dict[str, type], data: DataFrame, tags: List[str], action: str = "warn"
) -> None:
    for detector in (
        detect_correct_schema,
        detect_null_columns,
        detect_stale_columns,
        detect_zero_columns,
    ):
        detector(schema, data, tags, action)
