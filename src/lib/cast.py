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
import datetime
import warnings
import pandas
from typing import Any, Callable, Optional


def safe_float_cast(value: Any) -> Optional[float]:
    if value is None:
        return None
    if pandas.isna(value):
        return None
    if isinstance(value, int):
        return float(value)
    if isinstance(value, float):
        return value
    if value == "":
        return None
    try:
        value = str(value)
        value = re.sub(r",", "", value)
        value = re.sub(r"−", "-", value)
        return float(value)
    except:
        return None


def safe_int_cast(value: Any, round_function: Callable[[float], int] = round) -> Optional[int]:
    value = safe_float_cast(value)
    if value is None:
        return None
    try:
        value = round_function(value)
        return value
    except:
        return None


def safe_datetime_parse(
    value: str, date_format: str = None, warn: bool = False
) -> Optional[datetime.datetime]:
    try:
        return (
            datetime.datetime.fromisoformat(str(value))
            if not date_format
            else datetime.datetime.strptime(str(value), date_format)
        )
    except ValueError as exc:
        if warn:
            warnings.warn("Could not parse date {} using format {}".format(value, date_format))
        return None


def column_convert(series: pandas.Series, dtype: type) -> pandas.Series:
    if dtype == pandas.Int64Dtype():
        return series.apply(safe_int_cast).astype(dtype)
    if dtype == float:
        return series.apply(safe_float_cast).astype(dtype)
    if dtype == str:
        return series.fillna("").astype(str)

    raise ValueError("Unsupported dtype %r" % dtype)


def age_group(age: int, bin_count: int = 10, max_age: int = 100) -> str:
    """
    Categorical age group given a specific age, codified into a function to enforce consistency.
    """
    bin_size = max_age // bin_count
    if age >= max_age - bin_size:
        return f"{max_age - bin_size}-"

    boundaries = [(i * bin_size, (i + 1) * bin_size - 1) for i in range(bin_count - 1)]
    for a, b in boundaries:
        if age >= a and age <= b:
            return f"{a}-{b}"

    return None
