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
"""Downloads and converts raw data into a standardized format."""

import os
import pathlib

from absl import app
from absl import flags
from absl import logging
from dm_c19_modelling.england_data import error_reporting_util
from dm_c19_modelling.england_data import standardize_data

FLAGS = flags.FLAGS

flags.DEFINE_string(
    "scrape_date", None, "Enter in the form YYYYMMDD, eg. "
    "November 5, 2020 would be '20201105'. If you want the "
    "latest date, enter 'latest'.")
flags.DEFINE_string("raw_data_directory", None,
                    "The directory where the raw data is saved.")
flags.DEFINE_string("output_directory", None,
                    "The directory to write the standardized data .csv to.")
flags.DEFINE_boolean(
    "require_all_data", True,
    "Requires all data to be found, generates an error if not.")
flags.mark_flags_as_required(
    ["output_directory", "raw_data_directory", "scrape_date"])


@error_reporting_util.report_exception
def main(argv):
  if len(argv) > 1:
    raise app.UsageError("Too many command-line arguments.")

  logging.info("Standardizing data for scrape date: %s", FLAGS.scrape_date)

  paths_dict, scrape_date_dirname, missing_names = standardize_data.get_paths_for_given_date(
      FLAGS.raw_data_directory, FLAGS.scrape_date)
  formatted_dfs = standardize_data.format_raw_data_files(paths_dict)
  merged_df = standardize_data.merge_formatted_data(formatted_dfs)
  population_df = standardize_data.load_population_dataframe(
      FLAGS.raw_data_directory)

  if FLAGS.require_all_data:
    # Time varying data.
    missing_names.remove("population")
    if missing_names:
      missing_string = "' '".join(sorted(missing_names))
      raise FileNotFoundError(f"Could not find labels: '{missing_string}'")
    # Static population data.
    if population_df is None:
      raise FileNotFoundError("Could not find: population")

  logging.info("Data formatted and merged. Saving...")

  # Save the output.
  output_directory = pathlib.Path(FLAGS.output_directory) / scrape_date_dirname
  os.makedirs(output_directory, exist_ok=True)
  # Save individual formatted dfs.
  for name, df in formatted_dfs.items():
    path = output_directory / (name + ".csv")
    df.to_csv(path, index=False)
    logging.info("Wrote '%s' to '%s'", name, path)
  # Save merged standardized df.
  path = output_directory / "merged.csv"
  merged_df.to_csv(path, index=False)
  logging.info("Wrote merged data to '%s'", path)
  if population_df is not None:
    # Save population df.
    path = output_directory / "population.csv"
    population_df.to_csv(path, index=False)
    logging.info("Wrote population data to '%s'", path)


if __name__ == "__main__":
  app.run(main)
