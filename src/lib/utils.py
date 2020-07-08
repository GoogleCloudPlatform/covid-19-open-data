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

import os
from pathlib import Path
from functools import partial, reduce
from typing import Any, Callable, List, Dict, Tuple, Optional
from numpy import unique
from pandas import DataFrame, Series, concat, isna, merge
from pandas.api.types import is_numeric_dtype
from .io import fuzzy_text, pbar, tqdm

SRC = Path(os.path.dirname(__file__)) / ".."
URL_OUTPUTS_PROD = "https://storage.googleapis.com/covid19-open-data/v2"
CACHE_URL = "https://raw.githubusercontent.com/open-covid-19/data/cache"


def get_or_default(dict_like: Dict, key: Any, default: Any):
    return dict_like[key] if key in dict_like and not isna(dict_like[key]) else default


def pivot_table(data: DataFrame, pivot_name: str = "pivot", value_name: str = "value") -> DataFrame:
    """ Put a table in our preferred format when the regions are columns and date is index """
    dates = data.index.tolist() * len(data.columns)
    pivots: List[str] = sum([[name] * len(column) for name, column in data.iteritems()], [])
    values: List[Any] = sum([column.tolist() for name, column in data.iteritems()], [])
    records = zip(dates, pivots, values)
    return DataFrame.from_records(records, columns=[data.index.name, pivot_name, value_name])


def pivot_table_date_columns(
    data: DataFrame, pivot_name: str = "date", value_name: str = "value"
) -> DataFrame:
    """ Put a table in time series format when the dates are columns and keys are index """
    records = []
    for idx, row in data.iterrows():
        for pivot in data.columns:
            records.append({"index": idx, pivot_name: pivot, value_name: row[pivot]})
    return DataFrame.from_records(records).set_index("index")


def table_rename(
    data: DataFrame,
    column_adapter: Dict[str, str],
    remove_regex: str = r"[^a-z\s]",
    drop: bool = False,
) -> DataFrame:
    """
    Renames the table columns after converting all names to ASCII

    Args:
        data: The DataFrame containing columns to be renamed
        column_adapter: Dictionary of old -> new column name replacements
        remove_regex: Characters to ignore (and delete) during column renaming
        drop: Drop all columns not in the adapter in the output DataFrame

    Returns:
        DataFrame: A copy of the input DataFrame with new column names
    """
    data = data.copy()
    ascii_name = partial(fuzzy_text, remove_regex=remove_regex, remove_spaces=False)
    data.columns = [ascii_name(col) for col in data.columns]
    data = data.rename(
        columns={ascii_name(name_old): name_new for name_old, name_new in column_adapter.items()}
    )
    if drop:
        data = data[[col for col in data.columns if col in column_adapter.values()]]
    return data


def table_multimerge(dataframes: List[DataFrame], **merge_opts) -> DataFrame:
    """
    Merge multiple dataframes into a single one. This method assumes that all dataframes have at
    least one column in common.
    """
    return reduce(lambda df1, df2: merge(df1, df2, **merge_opts), dataframes)


def agg_last_not_null(series: Series, progress_bar: Optional[tqdm] = None) -> Series:
    """ Aggregator function used to keep the last non-null value in a list of rows """
    if progress_bar:
        progress_bar.update()
    return reduce(lambda x, y: y if not isna(y) else x, series)


def combine_tables(
    tables: List[DataFrame], keys: List[str], progress_label: str = None
) -> DataFrame:
    """ Combine a list of tables, keeping the last non-null value for every column """
    data = concat(tables)
    grouped = data.groupby([col for col in keys if col in data.columns])
    if not progress_label:
        return grouped.aggregate(agg_last_not_null).reset_index()
    else:
        progress_bar = pbar(
            total=len(grouped) * len(data.columns), desc=f"Combine {progress_label} outputs"
        )
        agg_func = partial(agg_last_not_null, progress_bar=progress_bar)
        combined = grouped.aggregate(agg_func).reset_index()
        progress_bar.n = len(grouped) * len(data.columns)
        progress_bar.refresh()
        return combined


def drop_na_records(table: DataFrame, keys: List[str]) -> DataFrame:
    """ Drops all records which have no data outside of the provided keys """
    value_columns = [col for col in table.columns if not col in keys]
    return table.dropna(subset=value_columns, how="all")


def grouped_transform(
    data: DataFrame,
    keys: List[str],
    transform: Callable,
    skip: List[str] = None,
    prefix: Tuple[str, str] = None,
) -> DataFrame:
    """
    Computes the transform for each item within the group indexed by `keys`.

    Args:
        data: The DataFrame to which transformations will be applied to
        keys: Columns to group the data by before applying the transformation
        transform: The function to be passed to the GroupBy.apply(...) call
        skip: Columns which should be kept as-is and not transformed
        prefix: Tuple used as a prefix for the name of the new transformed columns. The first
            element of the tuple will be the prefix of the pre-transformed columns, and the second
            element will be the prefix of the post-transformed columns.

    Returns:
        DataFrame: Data after the given transformation is applied to the relevant columns.
    """
    assert keys[-1] == "date", '"date" key should be last'

    # Keep a copy of the columns that will not be transformed
    data = data.sort_values(keys)
    skip = [] if skip is None else skip
    data_skipped = {col: data[col].copy() for col in skip if col in data}

    group = data.groupby(keys[:-1])
    prefix = ("", "") if prefix is None else prefix
    value_columns = [column for column in data.columns if column not in keys + skip]

    data = data.dropna(subset=value_columns, how="all").copy()
    for column in value_columns:
        if column in skip:
            continue
        if data[column].isnull().all():
            continue
        data[prefix[0] + column.replace(prefix[1], "")] = group[column].apply(transform)

    # Restore the columns that were not transformed
    for name, col in data_skipped.items():
        data[name] = col

    return data


def grouped_diff(
    data: DataFrame,
    keys: List[str],
    skip: List[str] = None,
    prefix: Tuple[str, str] = ("new_", "total_"),
) -> DataFrame:
    return grouped_transform(data, keys, lambda x: x.ffill().diff(), skip=skip, prefix=prefix)


def grouped_cumsum(
    data: DataFrame,
    keys: List[str],
    skip: List[str] = None,
    prefix: Tuple[str, str] = ("total_", "new_"),
) -> DataFrame:
    return grouped_transform(data, keys, lambda x: x.fillna(0).cumsum(), skip=skip, prefix=prefix)


def stack_table(
    data: DataFrame, index_columns: List[str], value_columns: List[str], stack_columns: List[str]
) -> DataFrame:
    """
    Pivots a DataFrame's columns and aggregates the result as new columns with suffix. E.g.:

    data:

    idx piv_1 piv_2 val
     0    A     X   1
     0    B     Y   2
     1    A     X   3
     1    B     Y   4

    stack_table(data, index_columns=[idx], value_columns=[val], stack_columns=[piv]):

    idx val val_A val_B val_X val_Y
     0   3    1     2     1     2
     1   7    3     4     3     4

    Note that this is not equivalent to the cartesian product of `stack_columns` as seen in the
    example above.
    """
    output = data.drop(columns=stack_columns).groupby(index_columns).sum()

    # Stash columns which are not part of the columns being indexed, aggregated or stacked
    used_columns = index_columns + value_columns + stack_columns
    stash_columns = [col for col in data.columns if col not in used_columns]
    stash_output = data[stash_columns].copy()
    data = data.drop(columns=stash_columns)

    # Aggregate (stack) columns with respect to the value columns
    for col_stack in stack_columns:
        col_stack_values = data[col_stack].dropna().unique()
        for col_variable in value_columns:
            df = data[index_columns + [col_variable, col_stack]].copy()
            df = df.pivot_table(
                values=col_variable, index=index_columns, columns=[col_stack], aggfunc="sum"
            )
            column_mapping = {suffix: f"{col_variable}_{suffix}" for suffix in col_stack_values}
            df = df.rename(columns=column_mapping)
            transfer_columns = list(column_mapping.values())
            output[transfer_columns] = df[transfer_columns]

    # Restore the stashed columns, reset index and return
    output[stash_columns] = stash_output
    return output.reset_index()


def filter_index_columns(data_columns: List[str], index_columns: List[str]) -> List[str]:
    """ Private function used to infer columns that this table should be indexed by """
    index_columns = [col for col in data_columns if col in index_columns]
    return index_columns + (["date"] if "date" in data_columns else [])


def filter_output_columns(columns: List[str], output_schema: Dict[str, str]) -> List[str]:
    """ Private function used to infer columns which are part of the output """
    return [col for col in columns if col in output_schema.keys()]


def infer_new_and_total(data: DataFrame) -> DataFrame:
    """
    We use the prefixes "new_" and "total_" as a convention to declare that a column contains values
    which are daily and cumulative, respectively. This helper function will infer daily values when
    only cumulative values are provided (by computing the daily difference) and, conversely, it will
    also infer cumulative values when only daily values are provided (by computing the cumsum).

    Args:
        data: The input table which may have some rows with "new_" but no "total_" values, or
            vice-versa

    Returns:
        DataFrame: Same as input data with all possible "new_" and "total_" values filled
    """

    # This function is only called as part of the pipeline processing, so we can assume that:
    # 1. All records with no key have been discarded by now
    # 2. All records have a date, since otherwise "new" vs "total" would not make sense
    index_columns = ["key", "date"]

    # We only care about columns which have prefix new_ and total_
    prefix_search = ("new_", "total_")
    value_columns = [
        col for col in data.columns if any(col.startswith(prefix) for prefix in prefix_search)
    ]

    # Perform the cumsum of columns which only have new_ values
    tot_columns = [
        col
        for col in value_columns
        if col.startswith("total_") and col.replace("total_", "new_") not in value_columns
    ]
    if tot_columns:
        new_data = grouped_diff(data[index_columns + tot_columns], keys=index_columns).drop(
            columns=index_columns
        )
        data[new_data.columns] = new_data

    # Perform the diff of columns which only have total_ values
    new_columns = [
        col
        for col in value_columns
        if col.startswith("new_") and col.replace("new_", "total_") not in value_columns
    ]
    if new_columns:
        tot_data = grouped_cumsum(data[index_columns + new_columns], keys=index_columns).drop(
            columns=index_columns
        )
        data[tot_data.columns] = tot_data

    return data


def stratify_age_and_sex(data: DataFrame) -> DataFrame:
    """
    Some data sources contain age and sex information. The output tables enforce that each record
    must have a unique <key, date> pair (or `key` if no `date` field is present). To solve this
    problem without losing the age and sex information, additional columns are created. For example,
    an input table might have columns [key, date, population, sex] and this function would produce
    the output [key, date, population, population_male, population_female].
    """

    # This function is only called as part of the pipeline processing, so we can assume that:
    # 1. All records with no key have been discarded by now
    # 2. All records must be indexable by key or <key, date>
    index_columns = ["key"] + (["date"] if "date" in data.columns else [])

    # Stratifying only makes sense for numeric columns, ignore the rest
    value_columns = [
        col for col in data.columns if col not in ("age", "sex") and is_numeric_dtype(data[col])
    ]

    # Stratified age uses a prefix since it's less obvious from the value names
    age_prefix = "age_"
    if "age" in data.columns:
        data.age = age_prefix + data.age

    # Determine the columns to stack depending on what's available
    stack_columns = []
    if "age" in data.columns:
        stack_columns += ["age"]
    if "sex" in data.columns:
        stack_columns += ["sex"]

    # Stack the columns which give us a stratified view of the data
    data = stack_table(
        data, index_columns=index_columns, value_columns=value_columns, stack_columns=stack_columns
    )

    # Age ranges are not uniform, so we add a helper variable which indicates the actual range and
    # make sure that the columns which contain the counts are uniform across all sources
    age_columns = {col: col.split(age_prefix, 2) for col in data.columns if age_prefix in col}
    age_buckets = unique([bucket for _, bucket in age_columns.values()])
    age_buckets_map = {bucket: f"{idx:02d}" for idx, bucket in enumerate(sorted(age_buckets))}
    data = data.rename(
        columns={
            col_name_old: f"{prefix}{age_prefix}{age_buckets_map[bucket]}"
            for col_name_old, (prefix, bucket) in age_columns.items()
        }
    )

    # Add helper columns to indicate range, assuming all variables have the same buckets
    for bucket_range, bucket_name in age_buckets_map.items():
        data[f"age_bin_{bucket_name}"] = bucket_range

    return data
