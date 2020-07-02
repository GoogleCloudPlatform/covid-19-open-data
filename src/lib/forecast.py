#!/usr/bin/env python
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
from functools import partial

import numpy as np
import pandas as pd
from tqdm import tqdm
from scipy import optimize


def _get_outbreak_mask(data: pd.DataFrame, threshold: int = 10):
    """ Returns a mask for > N confirmed cases. Used to filter out uninteresting dates """
    return data["Confirmed"] > threshold


def _logistic_function(X: float, a: float, b: float, c: float):
    """
    Used for prediction model. Uses the function:
    `f(x) = a * e^(-b * e^(-cx))`
    """
    return a * np.exp(-b * np.exp(-c * X))


def _forward_indices(indices: list, window: int):
    """ Adds `window` indices to a list of dates """
    date_indices = [datetime.date.fromisoformat(idx) for idx in indices]
    for _ in range(window):
        date_indices.append(date_indices[-1] + datetime.timedelta(days=1))
    return [idx.isoformat() for idx in date_indices]


def _compute_forecast(data: pd.Series, window: int):
    """
    Perform a forecast of `window` days past the last day of `data`, including a model estimate of
    all days already existing in `data`.
    """

    # Some of the parameter fittings result in overflow
    np.seterr(all="ignore")

    # Perform a simple fit of all available data up to this date
    X, y = list(range(len(data))), data.tolist()
    # Providing a reasonable initial guess is crucial for this model
    params, _ = optimize.curve_fit(
        _logistic_function, X, y, maxfev=int(1e6), p0=[max(y), np.median(X), 0.1]
    )

    # Append N new days to our indices
    date_indices = _forward_indices(data.index, window)

    # Perform projection with the previously estimated parameters
    projected = [_logistic_function(x, *params) for x in range(len(X) + window)]
    return pd.Series(projected, index=date_indices, name="Estimated")


def _compute_record_key(record: dict):
    """ Outputs the primary key for a dataframe row """
    region_code = record.get("RegionCode")
    country_code = record["CountryCode"]
    key_suffix = "" if not region_code or pd.isna(region_code) else "_%s" % region_code
    return country_code + key_suffix


def main(df):
    # Parse parameters
    PREDICT_WINDOW = 7
    DATAPOINT_COUNT = 28 + PREDICT_WINDOW

    # Read data from the open COVID-19 dataset
    df = df.set_index("Date")

    # Loop through each unique combination of country / region
    def map_func(df, key: str):

        # Filter dataset
        cols = ["Key", "Confirmed"]
        # Get data only for the selected country / region
        subset = df[df["Key"] == key][cols]
        # Get data only after the outbreak begun
        subset = subset[_get_outbreak_mask(subset)]
        # Early exit: no outbreak found
        if not len(subset):
            return []
        # Get a list of dates for existing data
        date_range = map(
            lambda datetime: datetime.date().isoformat(),
            pd.date_range(subset.index[0], subset.index[-1]),
        )

        # Forecast date is equal to the date of the last known datapoint, unless manually supplied
        forecast_date = max(subset.index)
        subset = subset[subset.index <= forecast_date].sort_index()

        # Sometimes our data appears to have duplicate values for specific cases, work around that
        subset = subset.query("~index.duplicated()")

        # Early exit: If there are less than DATAPOINT_COUNT output datapoints
        if len(subset) < DATAPOINT_COUNT - PREDICT_WINDOW:
            return []

        # Perform forecast
        forecast_data = _compute_forecast(subset["Confirmed"], PREDICT_WINDOW)

        # Capture only the last DATAPOINT_COUNT days
        forecast_data = forecast_data.sort_index().iloc[-DATAPOINT_COUNT:]

        # Fill out the corresponding index in the output forecast
        return [
            {
                "Key": key,
                "Date": idx,
                "ForecastDate": forecast_date,
                "Estimated": forecast_data.loc[idx],
                "Confirmed": int(subset.loc[idx, "Confirmed"]) if idx in subset.index else None,
            }
            for idx in forecast_data.index
        ]

    records = []
    map_func = partial(map_func, df)
    iter_len = len(df.Key.unique())
    for result in tqdm(map(map_func, df.Key.unique()), total=iter_len, desc="Computing forecast"):
        records += result

    # Do data cleanup here
    data = pd.DataFrame.from_records(records)
    forecast_columns = ["ForecastDate", "Date", "Key", "Estimated", "Confirmed"]
    data = data.sort_values(["Key", "Date"])[forecast_columns]

    # Output resulting dataframe
    return data
