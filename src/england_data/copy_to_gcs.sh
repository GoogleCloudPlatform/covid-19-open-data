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

# This script copies processed data stored locally to Cloud's GCS bucket.
#!/bin/bash

set -e

date="unspecified"  # Unspecified date.
local_dir="data"  # Default local output directory.
while getopts ":o:" opt; do
  case ${opt} in
    o )
      local_dir=$OPTARG
      ;;
    \? )
      echo "Usage: copy_to_gcs.sh [-o] <local directory>"
      ;;
    : )
      echo "Invalid option: $OPTARG requires an argument" 1>&2
      ;;
  esac
done
shift $((OPTIND -1))


DM_C19_DATA_DIRNAME=covid19-open-data/dm-c19-data
GS_PATH=gs://${DM_C19_DATA_DIRNAME}  # Web-accessible at: https://storage.googleapis.com/${DM_C19_DATA_DIRNAME}
STANDARDIZED_DIRNAME=standardized
MERGEABLE_DIRNAME=mergeable

# Copy processed data back to Cloud's dataset.
local_standardized_dir=${local_dir}/${STANDARDIZED_DIRNAME}
local_mergeable_dir=${local_dir}/${MERGEABLE_DIRNAME}
gs_standardized_dir=${GS_PATH}/${STANDARDIZED_DIRNAME}
gs_mergeable_dir=${GS_PATH}/${MERGEABLE_DIRNAME}
gsutil -m rsync -r -u ${local_standardized_dir} ${gs_standardized_dir}
gsutil -m rsync -r -u ${local_mergeable_dir} ${gs_mergeable_dir}
