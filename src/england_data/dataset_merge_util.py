# pylint: disable=g-bad-file-header
# Copyright 2020 DeepMind Technologies Limited. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or  implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ============================================================================

"""Merge England data to GCP's Covid-19 Open Data format: https://github.com/GoogleCloudPlatform/covid-19-open-data."""

import re
from typing import Tuple

from dm_c19_modelling.england_data import constants
import pandas as pd

_DATE = "date"

_KEY = "key"
_COUNTRY_CODE = "country_code"
_SUBREGION1_CODE = "subregion1_code"
_SUBREGION1_NAME = "subregion1_name"
_SUBREGION2_CODE = "subregion2_code"
_SUBREGION2_NAME = "subregion2_name"
_LOCALITY_CODE = "locality_code"
_LOCALITY_NAME = "locality_name"
_INDEX_DEFAULT_COLUMNS = {
    _KEY: None,
    "wikidata": float("nan"),
    "datacommons": float("nan"),
    _COUNTRY_CODE: "GB",
    "country_name": "United Kingdom",
    _SUBREGION1_CODE: "ENG",
    _SUBREGION1_NAME: "England",
    _SUBREGION2_CODE: float("nan"),
    _SUBREGION2_NAME: float("nan"),
    _LOCALITY_CODE: float("nan"),
    _LOCALITY_NAME: float("nan"),
    "3166-1-alpha-2": "GB",
    "3166-1-alpha-3": "GBR",
    "aggregation_level": 3,
}

_COLUMNS = constants.Columns


def _create_index_row_for_trust(regions_row: pd.Series) -> pd.Series:
  index_dict = _INDEX_DEFAULT_COLUMNS.copy()
  index_dict[_LOCALITY_CODE] = regions_row[_COLUMNS.REG_TRUST_CODE.value]
  index_dict[_LOCALITY_NAME] = regions_row[_COLUMNS.REG_TRUST_NAME.value]
  index_dict[_KEY] = (f"{index_dict[_COUNTRY_CODE]}_"
                      f"{index_dict[_SUBREGION1_CODE]}_"
                      f"{index_dict[_LOCALITY_CODE]}")
  return pd.Series(index_dict)


def _create_index_row_for_ltla(regions_row: pd.Series) -> pd.Series:
  index_dict = _INDEX_DEFAULT_COLUMNS.copy()
  index_dict[_LOCALITY_CODE] = regions_row[_COLUMNS.REG_LTLA_CODE.value]
  index_dict[_LOCALITY_NAME] = regions_row[_COLUMNS.REG_LTLA_NAME.value]
  index_dict[_KEY] = (f"{index_dict[_COUNTRY_CODE]}_"
                      f"{index_dict[_SUBREGION1_CODE]}_"
                      f"{index_dict[_LOCALITY_CODE]}")
  return pd.Series(index_dict)


def _create_index_row_for_ccg(regions_row: pd.Series) -> pd.Series:
  index_dict = _INDEX_DEFAULT_COLUMNS.copy()
  index_dict[_LOCALITY_CODE] = regions_row[_COLUMNS.REG_CCG_CODE.value]
  index_dict[_LOCALITY_NAME] = regions_row[_COLUMNS.REG_CCG_NAME.value]
  index_dict[_KEY] = (f"{index_dict[_COUNTRY_CODE]}_"
                      f"{index_dict[_SUBREGION1_CODE]}_"
                      f"{index_dict[_LOCALITY_CODE]}")
  return pd.Series(index_dict)


def _create_region_index(
    df: pd.DataFrame,
    data_type: constants.DataTypes) -> Tuple[pd.DataFrame, pd.DataFrame]:
  """Creates a region index dataframe from an input dataframe."""
  if data_type == constants.DataTypes.DAILY_DEATHS:
    create_index = _create_index_row_for_trust
  elif data_type == constants.DataTypes.DAILY_CASES:
    create_index = _create_index_row_for_ltla
  elif data_type == constants.DataTypes.ONLINE_111_AND_CALLS_111_999:
    create_index = _create_index_row_for_ccg
  elif data_type == constants.DataTypes.POPULATION:
    create_index = _create_index_row_for_ccg
  else:
    raise ValueError(f"Unknown data_type: '{data_type}'")
  reg_columns = [
      col for col in df.columns if col.startswith(constants.REGION_PREFIX)
  ]
  index_rows = []
  key_mapping = {}
  for _, regions_row in df[reg_columns].drop_duplicates().iterrows():
    index_row = create_index(regions_row)
    index_rows.append(index_row)
    key_mapping[tuple(regions_row)] = index_row[_KEY]
  index = pd.DataFrame(index_rows)
  # Create a new column for the input df which contains the generated keys.
  key_column = df[reg_columns].apply(
      lambda row: key_mapping[tuple(row)], axis=1)
  # Create a modified df which replaces the region columns with the key.
  keyed_df = df.copy()
  key_column_iloc = 1 if _DATE in keyed_df.columns else 0
  keyed_df.insert(key_column_iloc, _KEY, key_column)
  keyed_df.drop(reg_columns, axis=1, inplace=True)
  return index, keyed_df


def _pool_population_data_to_match_cloud(df: pd.DataFrame) -> pd.DataFrame:
  """Pool our population data columns to match Cloud's."""
  pooling = {
      "obs_population": r"^obs_population_[mf]_\d{2}$",
      "obs_population_male": r"^obs_population_m_\d{2}$",
      "obs_population_female": r"^obs_population_f_\d{2}$",
      "obs_population_age_00_09": r"^obs_population_[mf]_0\d$",
      "obs_population_age_10_19": r"^obs_population_[mf]_1\d$",
      "obs_population_age_20_29": r"^obs_population_[mf]_2\d$",
      "obs_population_age_30_39": r"^obs_population_[mf]_3\d$",
      "obs_population_age_40_49": r"^obs_population_[mf]_4\d$",
      "obs_population_age_50_59": r"^obs_population_[mf]_5\d$",
      "obs_population_age_60_69": r"^obs_population_[mf]_6\d$",
      "obs_population_age_70_79": r"^obs_population_[mf]_7\d$",
      "obs_population_age_80_89": r"^obs_population_[mf]_8\d$",
      "obs_population_age_90_99": r"^obs_population_[mf]_9\d$",
      "obs_population_age_80_and_older": r"^obs_population_[mf]_[89]\d$"
  }
  pooled = {"key": df["key"]}
  for cloud_col, pat in pooling.items():
    columns_to_pool = [col for col in df if re.match(pat, col) is not None]
    pooled[cloud_col] = df[columns_to_pool].sum(axis=1)
  return pd.DataFrame(pooled)


def _strip_prefix_from_columns_inplace(df: pd.DataFrame, prefix: str) -> None:
  mapping = {col: col.split(prefix, maxsplit=1)[-1] for col in df.columns}
  df.rename(columns=mapping, inplace=True)


def _convert_daily_deaths_to_match_cloud(
    df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
  index_df, keyed_df = _create_region_index(df,
                                            constants.DataTypes.DAILY_DEATHS)
  keyed_df.rename(
      columns={_COLUMNS.OBS_DEATHS.value: "new_deceased"}, inplace=True)
  return keyed_df, index_df


def _convert_daily_cases_to_match_cloud(df):
  index_df, keyed_df = _create_region_index(df, constants.DataTypes.DAILY_CASES)
  keyed_df.rename(
      columns={_COLUMNS.OBS_CASES.value: "new_confirmed"}, inplace=True)
  return keyed_df, index_df


def _convert_online_111_and_calls_111_999_to_match_cloud(df):
  index_df, keyed_df = _create_region_index(
      df, constants.DataTypes.ONLINE_111_AND_CALLS_111_999)
  _strip_prefix_from_columns_inplace(keyed_df, constants.OBSERVATION_PREFIX)
  return keyed_df, index_df


def _convert_population_to_match_cloud(df, pool=True):
  index_df, keyed_df = _create_region_index(df, constants.DataTypes.POPULATION)
  if pool:
    keyed_df = _pool_population_data_to_match_cloud(keyed_df)
  _strip_prefix_from_columns_inplace(keyed_df, constants.OBSERVATION_PREFIX)
  return keyed_df, index_df


def convert_to_match_cloud(df: pd.DataFrame, data_type: constants.DataTypes,
                           **kwargs) -> Tuple[pd.DataFrame, pd.DataFrame]:
  """Create a Cloud-compatible data dataframe from an input dataframe.

  Args:
    df: the input dataframe.
    data_type: the type of data included in the dataframe.
    **kwargs: optional additional parameters to the conversion function.

  Returns:
    an index dataframe, mapping keys for regional units to regional
    information, and the original dataframe with the regions replaced with the
    new keys.
  """
  if data_type == constants.DataTypes.DAILY_DEATHS:
    convert = _convert_daily_deaths_to_match_cloud
  elif data_type == constants.DataTypes.DAILY_CASES:
    convert = _convert_daily_cases_to_match_cloud
  elif data_type == constants.DataTypes.ONLINE_111_AND_CALLS_111_999:
    convert = _convert_online_111_and_calls_111_999_to_match_cloud
  elif data_type == constants.DataTypes.POPULATION:
    convert = _convert_population_to_match_cloud
  else:
    raise ValueError(f"Unknown data_type: '{data_type}'")
  return convert(df, **kwargs)


def merge_index_dfs(dfs):
  return pd.concat(dfs).drop_duplicates().reset_index(drop=True)
