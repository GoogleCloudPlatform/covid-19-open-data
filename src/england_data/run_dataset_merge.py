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
"""Merge England data to the Covid-19 Open Data format."""

import datetime
import os
import pathlib

from absl import app
from absl import flags
from absl import logging
from dm_c19_modelling.england_data import constants
from dm_c19_modelling.england_data import dataset_merge_util
from dm_c19_modelling.england_data import error_reporting_util
import pandas as pd


FLAGS = flags.FLAGS

flags.DEFINE_string(
    "scrape_date", None, "Enter in the form YYYYMMDD, eg. "
    "November 5, 2020 would be '20201105'. If you want the "
    "latest date, enter 'latest'.")
flags.DEFINE_string("input_directory", None,
                    "The directory to read the standardized data .csvs from.")
flags.DEFINE_string("output_directory", None,
                    "The directory to write the merged data .csv to.")
flags.DEFINE_list(
    "names", None, "List of names: "
    f"{', '.join([data_type.value for data_type in constants.DataTypes])}"
)
flags.mark_flag_as_required("scrape_date")
flags.mark_flag_as_required("input_directory")
flags.mark_flag_as_required("output_directory")


@error_reporting_util.report_exception
def main(argv):
  if len(argv) > 1:
    raise app.UsageError("Too many command-line arguments.")

  scrape_date = FLAGS.scrape_date
  if scrape_date == "latest":
    logging.info("Merging data for 'latest' scrape date")
    scrape_date_dirname = max(os.listdir(FLAGS.input_directory))
  else:
    try:
      datetime.datetime.strptime(scrape_date, "%Y%m%d")
    except ValueError:
      raise ValueError("Date must be formatted: YYYYMMDD")
    scrape_date_dirname = scrape_date
    logging.info("Merging data for '%s' scrape date", scrape_date_dirname)

  if FLAGS.names is None:
    names = list(constants.DataTypes)
  else:
    names = [constants.DataTypes(name) for name in FLAGS.names]

  # Read standardized datasets.
  input_directory = pathlib.Path(FLAGS.input_directory) / scrape_date_dirname
  data_dfs = {}
  for name in names:
    path = input_directory / f"{name.value}.csv"
    data_dfs[name] = pd.read_csv(path)
  logging.info("Data loaded.")

  # Create index for each standardized data dataframe.
  dfs = {}
  index_dfs = {}
  for name, df in data_dfs.items():
    dfs[name.value], index_dfs[
        name.value] = dataset_merge_util.convert_to_match_cloud(df, name)
    if name == constants.DataTypes.POPULATION:
      dfs["population_unpooled"], _ = dataset_merge_util.convert_to_match_cloud(
          df, name, pool=False)

  # Combine index dfs into a single one.
  index_df = dataset_merge_util.merge_index_dfs(index_dfs.values())
  logging.info("Indices built.")

  # Save the output.
  output_directory = pathlib.Path(FLAGS.output_directory) / scrape_date_dirname
  os.makedirs(output_directory, exist_ok=True)
  # Save individual keyed dfs.
  for name, df in dfs.items():
    path = output_directory / f"{name}.csv"
    df.to_csv(path, index=False)
    logging.info("Wrote '%s' to '%s'", name, path)

  # Save merged index df.
  path = output_directory / "index.csv"
  index_df.to_csv(path, index=False)
  logging.info("Wrote index to '%s'", path)


if __name__ == "__main__":
  app.run(main)
