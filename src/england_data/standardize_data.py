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
"""Tools for pre-processing the data into individual, standardized formats."""

import collections
import datetime
import itertools
import os
import pathlib
import re
from typing import Callable, Dict, Set, Tuple

from absl import logging
from dm_c19_modelling.england_data import constants
import pandas as pd
import yaml


_PATH_FILENAME_REGEXES = "filename_regexes.yaml"

_COLUMNS = constants.Columns

_DATE_FORMAT = "%Y-%m-%d"


def _order_columns(df: pd.DataFrame) -> pd.DataFrame:
  """Orders the columns of the dataframe as: date, region, observations."""
  df.insert(0, _COLUMNS.DATE.value, df.pop(_COLUMNS.DATE.value))
  reg_columns = []
  obs_columns = []
  for col in df.columns[1:]:
    if col.startswith(constants.REGION_PREFIX):
      reg_columns.append(col)
    elif col.startswith(constants.OBSERVATION_PREFIX):
      obs_columns.append(col)
    else:
      raise ValueError(f"Unknown column: '{col}'")
  columns = [_COLUMNS.DATE.value] + reg_columns + obs_columns
  return df[columns]


def _raw_data_formatter_daily_deaths(filepath: str) -> pd.DataFrame:
  """Loads and formats daily deaths data."""
  sheet_name = "Tab4 Deaths by trust"
  header = 15
  df = pd.read_excel(filepath, sheet_name=sheet_name, header=header)
  # Drop rows and columns which are all nans.
  df.dropna(axis=0, how="all", inplace=True)
  df.dropna(axis=1, how="all", inplace=True)
  # Drop unneeded columns and rows.
  drop_columns = ["Total", "Awaiting verification"]
  up_to_mar_1_index = "Up to 01-Mar-20"
  if sum(i for i in df[up_to_mar_1_index] if isinstance(i, int)) == 0.0:
    drop_columns.append(up_to_mar_1_index)
  df.drop(columns=drop_columns, inplace=True)
  df = df[df["Code"] != "-"]
  # Melt the death counts by date into "Date" and "Death Count" columns.
  df = df.melt(
      id_vars=["NHS England Region", "Code", "Name"],
      var_name="Date",
      value_name="Death Count")
  # Rename the columns to their standard names.
  df.rename(
      columns={
          "Date": _COLUMNS.DATE.value,
          "Death Count": _COLUMNS.OBS_DEATHS.value,
          "Code": _COLUMNS.REG_TRUST_CODE.value,
          "Name": _COLUMNS.REG_TRUST_NAME.value,
          "NHS England Region": _COLUMNS.REG_NHSER_NAME.value,
      },
      inplace=True)

  _order_columns(df)

  df[_COLUMNS.DATE.value] = df[_COLUMNS.DATE.value].map(
      lambda x: x.strftime(_DATE_FORMAT))
  # Sort and clean up the indices before returning the final dataframe.
  df.sort_values([
      _COLUMNS.DATE.value,
      _COLUMNS.REG_TRUST_NAME.value,
      _COLUMNS.REG_TRUST_CODE.value,
  ],
                 inplace=True)
  df.reset_index(drop=True, inplace=True)
  if df.isna().any().any():
    raise ValueError("Formatted data 'daily_deaths' contains nans")
  return df


def _raw_data_formatter_daily_cases(filepath: str) -> pd.DataFrame:
  """Loads and formats daily cases data."""
  df = pd.read_csv(filepath)
  df.rename(columns={"Area type": "Area_type"}, inplace=True)
  df.query("Area_type == 'ltla'", inplace=True)
  # Drop unneeded columns and rows.
  drop_columns = [
      "Area_type", "Cumulative lab-confirmed cases",
      "Cumulative lab-confirmed cases rate"
  ]
  df.drop(columns=drop_columns, inplace=True)
  # Rename the columns to their standard names.
  df.rename(
      columns={
          "Area name": _COLUMNS.REG_LTLA_NAME.value,
          "Area code": _COLUMNS.REG_LTLA_CODE.value,
          "Specimen date": _COLUMNS.DATE.value,
          "Daily lab-confirmed cases": _COLUMNS.OBS_CASES.value,
      },
      inplace=True)

  _order_columns(df)

  # Sort and clean up the indices before returning the final dataframe.
  df.sort_values([
      _COLUMNS.DATE.value,
      _COLUMNS.REG_LTLA_NAME.value,
      _COLUMNS.REG_LTLA_CODE.value,
  ],
                 inplace=True)
  df.reset_index(drop=True, inplace=True)
  if df.isna().any().any():
    raise ValueError("Formatted data 'daily_cases' contains nans")
  return df


def _raw_data_formatter_google_mobility(filepath: str) -> pd.DataFrame:
  """Loads and formats Google mobility data."""
  df = pd.read_csv(filepath)
  # Filter to UK.
  df.query("country_region_code == 'GB'", inplace=True)
  # Drop unneeded columns and rows.
  drop_columns = [
      "country_region_code", "country_region", "metro_area", "census_fips_code"
  ]
  df.drop(columns=drop_columns, inplace=True)
  # Fill missing region info with "na".
  df[["sub_region_1", "sub_region_2", "iso_3166_2_code"]].fillna(
      "na", inplace=True)
  # Rename the columns to their standard names.
  df.rename(
      columns={
          "sub_region_1":
              _COLUMNS.REG_SUB_REGION_1.value,
          "sub_region_2":
              _COLUMNS.REG_SUB_REGION_2.value,
          "iso_3166_2_code":
              _COLUMNS.REG_ISO_3166_2_CODE.value,
          "date":
              _COLUMNS.DATE.value,
          "retail_and_recreation_percent_change_from_baseline":
              _COLUMNS.OBS_MOBILITY_RETAIL_AND_RECREATION.value,
          "grocery_and_pharmacy_percent_change_from_baseline":
              _COLUMNS.OBS_MOBILITY_GROCERY_AND_PHARMACY.value,
          "parks_percent_change_from_baseline":
              _COLUMNS.OBS_MOBILITY_PARKS.value,
          "transit_stations_percent_change_from_baseline":
              _COLUMNS.OBS_MOBILITY_TRANSIT_STATIONS.value,
          "workplaces_percent_change_from_baseline":
              _COLUMNS.OBS_MOBILITY_WORKPLACES.value,
          "residential_percent_change_from_baseline":
              _COLUMNS.OBS_MOBILITY_RESIDENTIAL.value,
      },
      inplace=True)

  _order_columns(df)

  # Sort and clean up the indices before returning the final dataframe.
  df.sort_values([
      _COLUMNS.DATE.value,
      _COLUMNS.REG_SUB_REGION_1.value,
      _COLUMNS.REG_SUB_REGION_2.value,
      _COLUMNS.REG_ISO_3166_2_CODE.value,
  ],
                 inplace=True)
  df.reset_index(drop=True, inplace=True)
  return df


def _raw_data_formatter_online_111(filepath: str) -> pd.DataFrame:
  """Loads and formats online 111 data."""
  df = pd.read_csv(filepath)
  # Drop nans.
  df.dropna(subset=["ccgcode"], inplace=True)
  # Reformat dates.
  remap_dict = {
      "journeydate":
          lambda x: datetime.datetime.strptime(x, "%d/%m/%Y").strftime(  # pylint: disable=g-long-lambda
              _DATE_FORMAT),
      "ccgname":
          lambda x: x.replace("&", "and"),
      "sex": {
          "Female": "f",
          "Male": "m",
          "Indeterminate": "u",
      },
      "ageband": {
          "0-18 years": "0",
          "19-69 years": "19",
          "70+ years": "70"
      }
  }
  for col, remap in remap_dict.items():
    df[col] = df[col].map(remap)

  journeydate_values = pd.date_range(
      df.journeydate.min(), df.journeydate.max()).strftime(_DATE_FORMAT)
  ccgcode_values = df.ccgcode.unique()
  df.sex.fillna("u", inplace=True)
  sex_values = ["f", "m", "u"]
  assert set(sex_values) >= set(df.sex.unique()), "unsupported sex value"
  df.ageband.fillna("u", inplace=True)
  ageband_values = ["0", "19", "70", "u"]
  assert set(ageband_values) >= set(
      df.ageband.unique()), "unsupported ageband value"

  ccg_code_name_map = df[["ccgcode", "ccgname"
                         ]].set_index("ccgcode")["ccgname"].drop_duplicates()
  # Some CCG codes have duplicate names, which differ by their commas. Keep the
  # longer ones.
  fn = lambda x: sorted(x["ccgname"].map(lambda y: (len(y), y)))[-1][1]
  ccg_code_name_map = ccg_code_name_map.reset_index().groupby("ccgcode").apply(
      fn)

  df_full = pd.DataFrame(
      list(
          itertools.product(journeydate_values, ccgcode_values, sex_values,
                            ageband_values)),
      columns=["journeydate", "ccgcode", "sex", "ageband"])
  df = pd.merge(df_full, df, how="outer")
  # 0 calls don't have rows, so are nans.
  df["Total"].fillna(0, inplace=True)
  df["ccgname"] = df["ccgcode"].map(ccg_code_name_map)

  # Combine sex and ageband columns into a joint column.
  df["sex_ageband"] = df["sex"] + "_" + df["ageband"]
  df = df.pivot_table(
      index=["journeydate", "ccgcode", "ccgname"],
      columns="sex_ageband",
      values="Total").reset_index()
  df.columns.name = None

  # Rename the columns to their standard names.
  df.rename(
      columns={
          "ccgcode": _COLUMNS.REG_CCG_CODE.value,
          "ccgname": _COLUMNS.REG_CCG_NAME.value,
          "journeydate": _COLUMNS.DATE.value,
          "f_0": _COLUMNS.OBS_ONLINE_111_F_0.value,
          "f_19": _COLUMNS.OBS_ONLINE_111_F_19.value,
          "f_70": _COLUMNS.OBS_ONLINE_111_F_70.value,
          "f_u": _COLUMNS.OBS_ONLINE_111_F_U.value,
          "m_0": _COLUMNS.OBS_ONLINE_111_M_0.value,
          "m_19": _COLUMNS.OBS_ONLINE_111_M_19.value,
          "m_70": _COLUMNS.OBS_ONLINE_111_M_70.value,
          "m_u": _COLUMNS.OBS_ONLINE_111_M_U.value,
          "u_0": _COLUMNS.OBS_ONLINE_111_U_0.value,
          "u_19": _COLUMNS.OBS_ONLINE_111_U_19.value,
          "u_70": _COLUMNS.OBS_ONLINE_111_U_70.value,
          "u_u": _COLUMNS.OBS_ONLINE_111_U_U.value,
      },
      inplace=True)

  _order_columns(df)

  # Sort and clean up the indices before returning the final dataframe.
  df.sort_values([
      _COLUMNS.DATE.value,
      _COLUMNS.REG_CCG_NAME.value,
      _COLUMNS.REG_CCG_CODE.value,
  ],
                 inplace=True)
  df.reset_index(drop=True, inplace=True)
  if df.isna().any().any():
    raise ValueError("Formatted data 'online_111' contains nans")
  return df


def _raw_data_formatter_calls_111_999(filepath: str) -> pd.DataFrame:
  """Loads and formats 111 & 999 calls data."""
  df = pd.read_csv(filepath)
  # Drop unneeded columns and rows.
  drop_columns = []
  df.drop(columns=drop_columns, inplace=True)
  # Drop nans.
  df.dropna(subset=["CCGCode", "CCGName"], inplace=True)

  # Reformat values.
  df["AgeBand"].fillna("u", inplace=True)
  remap_dict = {
      "Call Date":
          lambda x: datetime.datetime.strptime(x, "%d/%m/%Y").strftime(  # pylint: disable=g-long-lambda
              "%Y-%m-%d"),
      "CCGName":
          lambda x: x.replace("&", "and"),
      "SiteType":
          lambda x: str(int(x)),
      "Sex": {
          "Female": "f",
          "Male": "m",
          "Unknown": "u",
      },
      "AgeBand": {
          "0-18 years": "0",
          "19-69 years": "19",
          "70-120 years": "70",
          "u": "u",
      }
  }

  for col, remap in remap_dict.items():
    df[col] = df[col].map(remap)

  call_date_values = pd.date_range(df["Call Date"].min(),
                                   df["Call Date"].max()).strftime(_DATE_FORMAT)
  ccgcode_values = df["CCGCode"].unique()
  sitetype_values = ["111", "999"]
  assert set(sitetype_values) >= set(
      df.SiteType.unique()), "unsupported sitetype value"
  sex_values = ["f", "m", "u"]
  assert set(sex_values) >= set(df.Sex.unique()), "unsupported sex value"
  ageband_values = ["0", "19", "70", "u"]
  assert set(ageband_values) >= set(
      df.AgeBand.unique()), "unsupported ageband value"

  ccg_code_name_map = df[["CCGCode", "CCGName"
                         ]].set_index("CCGCode")["CCGName"].drop_duplicates()

  df_full = pd.DataFrame(
      list(itertools.product(call_date_values, ccgcode_values, sitetype_values,
                             sex_values, ageband_values)),
      columns=["Call Date", "CCGCode", "SiteType", "Sex", "AgeBand"])
  df = pd.merge(df_full, df, how="outer")
  # 0 calls don't have rows, so are nans.
  df["TriageCount"].fillna(0, inplace=True)
  df["CCGName"] = df["CCGCode"].map(ccg_code_name_map)

  # Combine SiteType, Sex, and AgeBand columns into a joint column.
  df["SiteType_Sex_AgeBand"] = (
      df["SiteType"] + "_" + df["Sex"] + "_" + df["AgeBand"])

  df = df.pivot_table(
      index=["Call Date", "CCGCode", "CCGName"],
      columns="SiteType_Sex_AgeBand",
      values="TriageCount").reset_index()
  df.columns.name = None

  # Rename the columns to their standard names.
  df.rename(
      columns={
          "CCGCode": _COLUMNS.REG_CCG_CODE.value,
          "CCGName": _COLUMNS.REG_CCG_NAME.value,
          "Call Date": _COLUMNS.DATE.value,
          "111_f_0": _COLUMNS.OBS_CALL_111_F_0.value,
          "111_f_19": _COLUMNS.OBS_CALL_111_F_19.value,
          "111_f_70": _COLUMNS.OBS_CALL_111_F_70.value,
          "111_f_u": _COLUMNS.OBS_CALL_111_F_U.value,
          "111_m_0": _COLUMNS.OBS_CALL_111_M_0.value,
          "111_m_19": _COLUMNS.OBS_CALL_111_M_19.value,
          "111_m_70": _COLUMNS.OBS_CALL_111_M_70.value,
          "111_m_u": _COLUMNS.OBS_CALL_111_M_U.value,
          "111_u_0": _COLUMNS.OBS_CALL_111_U_0.value,
          "111_u_19": _COLUMNS.OBS_CALL_111_U_19.value,
          "111_u_70": _COLUMNS.OBS_CALL_111_U_70.value,
          "111_u_u": _COLUMNS.OBS_CALL_111_U_U.value,
          "999_f_0": _COLUMNS.OBS_CALL_999_F_0.value,
          "999_f_19": _COLUMNS.OBS_CALL_999_F_19.value,
          "999_f_70": _COLUMNS.OBS_CALL_999_F_70.value,
          "999_f_u": _COLUMNS.OBS_CALL_999_F_U.value,
          "999_m_0": _COLUMNS.OBS_CALL_999_M_0.value,
          "999_m_19": _COLUMNS.OBS_CALL_999_M_19.value,
          "999_m_70": _COLUMNS.OBS_CALL_999_M_70.value,
          "999_m_u": _COLUMNS.OBS_CALL_999_M_U.value,
          "999_u_0": _COLUMNS.OBS_CALL_999_U_0.value,
          "999_u_19": _COLUMNS.OBS_CALL_999_U_19.value,
          "999_u_70": _COLUMNS.OBS_CALL_999_U_70.value,
          "999_u_u": _COLUMNS.OBS_CALL_999_U_U.value,
      },
      inplace=True)

  _order_columns(df)

  # Sort and clean up the indices before returning the final dataframe.
  df.sort_values([
      _COLUMNS.DATE.value,
      _COLUMNS.REG_CCG_NAME.value,
      _COLUMNS.REG_CCG_CODE.value,
  ],
                 inplace=True)
  df.reset_index(drop=True, inplace=True)
  if df.isna().any().any():
    raise ValueError("Formatted data 'calls_111_999' contains nans")
  return df


_FORMATTER_FUNCTIONS = {
    "daily_deaths": _raw_data_formatter_daily_deaths,
    "daily_cases": _raw_data_formatter_daily_cases,
    "google_mobility": _raw_data_formatter_google_mobility,
    "online_111": _raw_data_formatter_online_111,
    "calls_111_999": _raw_data_formatter_calls_111_999,
}


def _get_raw_data_formatter_by_name(name: str) -> Callable[[str], pd.DataFrame]:
  return _FORMATTER_FUNCTIONS[name]


def _merge_online_111_and_calls_111_999(
    df_online_111: pd.DataFrame,
    df_calls_111_999: pd.DataFrame) -> pd.DataFrame:
  """Merges the 111 online and 111/999 calls into a single dataframe."""
  df = pd.merge(
      df_online_111,
      df_calls_111_999,
      how="outer",
      on=[
          _COLUMNS.DATE.value,
          _COLUMNS.REG_CCG_CODE.value,
          _COLUMNS.REG_CCG_NAME.value,
      ])
  return df


def format_raw_data_files(
    paths_dict: Dict[str, str]) -> Dict[str, pd.DataFrame]:
  """Loads and formats the individual raw data files.

  Args:
    paths_dict: mapping from data names to filepaths.
  Returns:
    mapping from data names to formatted dataframes.
  """
  formatted_dfs = {}
  for name, path in paths_dict.items():
    logging.info("Formatting raw data: %s", name)
    formatter = _get_raw_data_formatter_by_name(name)
    formatted_dfs[name] = formatter(path)
  logging.info("Merging online 111 and 111/999 calls")
  if "online_111" and "calls_111_999" in formatted_dfs:
    formatted_dfs[
        "online_111_and_calls_111_999"] = _merge_online_111_and_calls_111_999(
            formatted_dfs.pop("online_111"), formatted_dfs.pop("calls_111_999"))
  elif "online_111" in formatted_dfs:
    formatted_dfs["online_111_and_calls_111_999"] = formatted_dfs.pop(
        "online_111")
  elif "calls_111_999" in formatted_dfs:
    formatted_dfs["online_111_and_calls_111_999"] = formatted_dfs.pop(
        "calls_111_999")
  return formatted_dfs


def merge_formatted_data(
    formatted_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
  """Concatenates all formatted data into a single dataframe.

  Args:
    formatted_data: mapping from the data name to its dataframe.
  Returns:
    a dataframe containing all of the input dataframes.
  """
  logging.info("Merging all dataframes")
  dfs = []
  for name, df in formatted_data.items():
    df = df.copy()
    df.insert(1, _COLUMNS.OBSERVATION_TYPE.value, name)
    dfs.append(df)
  df_merged = pd.concat(dfs)
  reg_columns = [
      c for c in df_merged.columns if c.startswith(constants.REGION_PREFIX)
  ]
  df_merged.sort_values(
      [_COLUMNS.DATE.value, _COLUMNS.OBSERVATION_TYPE.value] + reg_columns,
      inplace=True)
  df_merged.reset_index(drop=True, inplace=True)
  return df_merged


def _load_filename_regexes() -> Dict[str, str]:
  """Gets a mapping from the data name to the regex for that data's filepath."""
  path = pathlib.Path(os.path.dirname(
      os.path.realpath(__file__))) / _PATH_FILENAME_REGEXES
  with open(path) as fid:
    return yaml.load(fid, Loader=yaml.SafeLoader)


def get_paths_for_given_date(
    raw_data_directory: str,
    scrape_date: str) -> Tuple[Dict[str, str], str, Set[str]]:
  """Get the raw data paths for a scrape date and filename regex.

  Args:
    raw_data_directory: the directory where the raw data is saved.
    scrape_date: the scrape date to use, in the form YYYYMMDD, or 'latest'.
  Returns:
    mapping of data names to filepaths
    the scrape date used
    names whose data was not found on disk
  """
  filename_regexes = _load_filename_regexes()
  if scrape_date == "latest":
    rx = re.compile("^[0-9]{8}$")
    directories = []
    for filename in os.listdir(raw_data_directory):
      if rx.match(filename) is None:
        continue
      path = pathlib.Path(raw_data_directory) / filename
      if not os.path.isdir(path):
        continue
      directories.append(path)
    if not directories:
      raise ValueError("Could not find latest scrape date directory")
    directory = max(directories)
    scrape_date_dirname = directory.parts[-1]
  else:
    try:
      datetime.datetime.strptime(scrape_date, "%Y%m%d")
    except ValueError:
      raise ValueError("Date must be formatted: YYYYMMDD")
    scrape_date_dirname = scrape_date
    directory = pathlib.Path(raw_data_directory) / scrape_date_dirname

  paths_dict = collections.defaultdict(lambda: None)
  for name, filename_regex in filename_regexes.items():
    rx = re.compile(f"^{filename_regex}$")
    for filename in os.listdir(directory):
      path = directory / filename
      if os.path.isdir(path):
        continue
      match = rx.match(filename)
      if match is None:
        continue
      if paths_dict[name] is not None:
        raise ValueError("There should only be 1 file per name")
      paths_dict[name] = str(path)
  missing_names = set(filename_regexes.keys()) - set(paths_dict.keys())
  return dict(paths_dict), scrape_date_dirname, missing_names


def load_population_dataframe(raw_data_directory: str) -> pd.DataFrame:
  """Load population data from disk, and create a dataframe from it.

  Args:
    raw_data_directory: the directory where the raw data is saved.
  Returns:
    a dataframe containing population data.
  """
  filename = _load_filename_regexes()["population"]
  filepath = pathlib.Path(raw_data_directory) / filename

  kwargs = dict(header=0, skiprows=(0, 1, 2, 3, 4, 5, 7))
  try:
    pop_m = pd.read_excel(filepath, sheet_name="Mid-2019 Males", **kwargs)
    pop_f = pd.read_excel(filepath, sheet_name="Mid-2019 Females", **kwargs)
  except FileNotFoundError:
    return None

  # Remove lower resolution columns.
  columns_to_remove = ("STP20 Code", "STP20 Name", "NHSER20 Code",
                       "NHSER20 Name", "All Ages")
  for col in columns_to_remove:
    del pop_m[col]
    del pop_f[col]

  mapping = {"CCG Code": _COLUMNS.REG_CCG_CODE.value,
             "CCG Name": _COLUMNS.REG_CCG_NAME.value,
             "90+": 90}

  pop_m.rename(columns=mapping, inplace=True)
  pop_f.rename(columns=mapping, inplace=True)
  # This labels the male and female data uniquely so they can be merged.
  pop_m.rename(
      columns=lambda x: f"m_{str(x).lower()}" if isinstance(x, int) else x,
      inplace=True)
  pop_f.rename(
      columns=lambda x: f"f_{str(x).lower()}" if isinstance(x, int) else x,
      inplace=True)
  region_columns = [_COLUMNS.REG_CCG_NAME.value, _COLUMNS.REG_CCG_CODE.value]
  df = pd.merge(pop_m, pop_f, how="outer", on=tuple(region_columns))

  mapping = {
      f"{gender}_{age}":
      _COLUMNS.OBS_POPULATION_GENDER_AGE.value.format(gender=gender, age=age)
      for gender, age in itertools.product(("m", "f"), range(91))
  }
  df.rename(columns=mapping, inplace=True)

  return df
