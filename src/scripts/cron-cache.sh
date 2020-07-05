#!/bin/bash

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

#######################################
# Cache sources of data which only provide daily views so they can be aggregated
# Arguments:
#   Branch to use for the data pipeline execution. Default: "main".
#   GCS bucket name to upload results to; empty means do not upload. Default: "".
#######################################

# This is brittle but prevents from continuing in case of failure since we don't want to overwrite
# files in the server if anything went wrong
set -xe

# Parse the arguments
readonly GCS_OUTPUT_BUCKET=$1

# Clone the repo into a temporary directory
readonly TMPDIR=$(mktemp -d -t opencovid-$(date +%Y-%m-%d-%H-%M-%S)-XXXX)
git clone https://github.com/open-covid-19/data.git --single-branch -b main "$TMPDIR/opencovid"

# Build the Docker image which contains all of our dependencies
docker build "$TMPDIR/opencovid/src" -t opencovid

# Download the previous cache files into the working directory
mkdir -p "$TMPDIR/opencovid/output/cache"
if [ -z "$GCS_OUTPUT_BUCKET" ]
then
    echo "GCS output bucket not set, no previous cache files downloaded"
else
    gsutil -m cp -r "gs://$GCS_OUTPUT_BUCKET/cache" "$TMPDIR/opencovid/output/" || true
fi


# Run the cache command in a Docker instance using our Docker image
docker run -v "$TMPDIR/opencovid":/opencovid -w /opencovid -i opencovid:latest /bin/bash -s <<EOF
cd src
# Install Python and NodeJS dependencies
# Necessary if the deps changed since the Docker image was built
python3 -m pip install -r requirements.txt && yarn
# Cache all the data sources
python3 cache.py
EOF

# Known location where the output files are created
readonly OUTPUT_FOLDER="$TMPDIR/opencovid/output/cache"

# Upload the outputs to Google Cloud Storage
if [ -z "$GCS_OUTPUT_BUCKET" ]
then
    echo "GCS output bucket not set, skipping upload"
    echo "Output files located in $OUTPUT_FOLDER"
else
    readonly GCS_OUTPUT_PATH="gs://$GCS_OUTPUT_BUCKET/"

    # Only update the cached files if there is a GCS bucket
    echo "Uploading cache files to GCS bucket $GCS_OUTPUT_BUCKET"
    gsutil -m cp -r "$OUTPUT_FOLDER" "$GCS_OUTPUT_PATH"

    # Cleanup needs sudo because Docker creates files using its uid
    sudo rm -rf $TMPDIR
fi
