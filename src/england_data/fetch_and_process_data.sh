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

# This script fetches raw data from Covid-19 Open Data's GCS bucket, processes
# it, and stores it locally.
#!/bin/bash

set -e

local_output_dir="data"  # Default local output directory.
while getopts ":o:" opt; do
  case ${opt} in
    o )
      local_output_dir=$OPTARG
      ;;
    \? )
      echo "Usage: fetch_and_process_data.sh -o <local output directory>"
      ;;
    : )
      echo "Invalid option: $OPTARG requires an argument" 1>&2
      ;;
  esac
done
shift $((OPTIND -1))


DM_C19_DATA_DIRNAME=covid19-open-data/dm-c19-data
GS_PATH=gs://${DM_C19_DATA_DIRNAME}  # Web-accessible at: https://storage.googleapis.com/${DM_C19_DATA_DIRNAME}
RAW_DIRNAME=raw
STANDARDIZED_DIRNAME=standardized
MERGEABLE_DIRNAME=mergeable

POPULATION_FILENAME=SAPE22DT6a-mid-2019-ccg-2020-estimates-unformatted.xlsx


# Find dates that already have standardized/mergeable data so we can skip them.
# Find the dates.
get_gs_date_directories () {
  date_pat="^(20[2-9][0-9][0-1][0-9][0-3][0-9])$"
  for path in $(gsutil ls ${GS_PATH}/$1 | sort -r); do
    if [[ $(basename ${path}) =~ $date_pat ]]
    then
      echo ${BASH_REMATCH[0]}
    fi
  done
}
IFS=$'\n' gs_raw_dates=($(sort -r <<<"$(get_gs_date_directories ${RAW_DIRNAME})")); unset IFS
IFS=$'\n' gs_standardized_dates=($(sort -r <<<"$(get_gs_date_directories ${STANDARDIZED_DIRNAME})")); unset IFS
IFS=$'\n' gs_mergeable_dates=($(sort -r <<<"$(get_gs_date_directories ${MERGEABLE_DIRNAME})")); unset IFS
# Find missing standardized and mergeable dates.
missing_standardized_dates=()
missing_mergeable_dates=()
for date in "${gs_raw_dates[@]}"; do
  if ! [[ "${gs_standardized_dates[@]}" =~ "${date}" ]]; then
    missing_standardized_dates+=("${date}")
  fi
done
for date in "${gs_raw_dates[@]}"; do
  if ! [[ "${gs_mergeable_dates[@]}" =~ "${date}" ]]; then
    missing_mergeable_dates+=("${date}")
  fi
done

gs_raw_dir=${GS_PATH}/${RAW_DIRNAME}
local_raw_dir=${local_output_dir}/${RAW_DIRNAME}
echo -n "Fetching raw data from ${gs_raw_dir} to ${local_raw_dir} ... "
for date in "${missing_standardized_dates[@]}"; do
  local_dir=${local_raw_dir}/${date}
  mkdir -p ${local_dir}
  gsutil -m rsync -r ${gs_raw_dir}/${date} ${local_dir}
done
gsutil cp ${gs_raw_dir}/${POPULATION_FILENAME} ${local_raw_dir}
echo "Done."


local_standardized_dir=${local_output_dir}/${STANDARDIZED_DIRNAME}
echo -n "Standardizing data and saving to ${local_standardized_dir} ... "
for date in "${missing_standardized_dates[@]}"; do
  python3 run_standardize_data.py \
    --raw_data_directory=${local_raw_dir} \
    --output_directory=${local_standardized_dir} \
    --scrape_date=${date} \
    --require_all_data=False  # So the script still works if there's missing data.
done
echo "Done."


# Make standardized data mergeable with the Covid-19 Open Data dataset.
local_mergeable_dir=${local_output_dir}/${MERGEABLE_DIRNAME}
echo -n "Making standardized data mergeable with Covid-19 Open Data and saving to ${local_mergeable_dir} ... "
for date in "${missing_standardized_dates[@]}"; do
  python3 run_dataset_merge.py \
    --input_directory=${local_standardized_dir} \
    --output_directory=${local_mergeable_dir} \
    --scrape_date=${date}
done
echo "Done."
