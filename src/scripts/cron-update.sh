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
# Run data pipelines and upload outputs to GCS
# Arguments:
#   Branch to use for the data pipeline execution. Default: "main".
#   GCS bucket name to upload results to; empty means do not upload. Default: "".
#######################################

# This is brittle but prevents from continuing in case of failure since we don't want to overwrite
# files in the server if anything went wrong
set -xe

# Parse the arguments
readonly BRANCH="${1:-main}"
readonly GCS_OUTPUT_BUCKET=$2

# Clone the repo into a temporary directory
readonly TMPDIR=$(mktemp -d -t opencovid-$(date +%Y-%m-%d-%H-%M-%S)-XXXX)
git clone https://github.com/open-covid-19/data.git --single-branch -b $BRANCH "$TMPDIR/opencovid"

# Build the Docker image which contains all of our dependencies
docker build "$TMPDIR/opencovid/src" -t opencovid

# Download the intermediate files into the working directory
# We always use the production's intermediate files to ensure reproducibility
# This system seems brittle, but it lets us pick up the last successful output of any data source
# as long as it is declared in a pipeline's configuration in case the data source breaks.
readonly GCS_PROD_BUCKET="covid19-open-data"
mkdir -p "$TMPDIR/opencovid/output/snapshot"
mkdir -p "$TMPDIR/opencovid/output/intermediate"
gsutil -m cp -r "gs://$GCS_PROD_BUCKET/snapshot" "$TMPDIR/opencovid/output/" || true
gsutil -m cp -r "gs://$GCS_PROD_BUCKET/intermediate" "$TMPDIR/opencovid/output/" || true

# Run the update command in a Docker instance using our Docker image
docker run -v "$TMPDIR/opencovid":/opencovid -w /opencovid -i opencovid:latest /bin/bash -s <<EOF
cd src
# Install Python dependencies
# Necessary if the deps changed since the Docker image was built
python3 -m pip install -r requirements.txt
# Update all the data pipelines
python3 update.py --no-progress
# Get the files ready for publishing
python3 publish.py
EOF

# Known location where the output files are created
readonly OUTPUT_FOLDER="$TMPDIR/opencovid/output/public/v2"

# Upload the outputs to Google Cloud Storage
if [ -z "$GCS_OUTPUT_BUCKET" ]
then
    echo "GCS output bucket not set, skipping upload"
    echo "Output files located in $OUTPUT_FOLDER"
else
    if [ $BRANCH == "main" ]
    then
        # Outputs from the main branch go into the root folder
        readonly GCS_OUTPUT_PATH="gs://$GCS_OUTPUT_BUCKET/"

        # Only update the intermediate files if this is for the main branch
        echo "Uploading intermediate files to GCS bucket $GCS_OUTPUT_BUCKET"
        gsutil -m cp -r "$TMPDIR/opencovid/output/snapshot" "$GCS_OUTPUT_BUCKET/"
        gsutil -m cp -r "$TMPDIR/opencovid/output/intermediate" "$GCS_OUTPUT_BUCKET/"
    else
        # Outputs from any other branch go into the staging folder
        readonly GCS_OUTPUT_PATH="gs://$GCS_OUTPUT_BUCKET/staging/$BRANCH/"
    fi

    echo "Uploading outputs to GCS path $GCS_OUTPUT_PATH"
    gsutil -m cp -r "$OUTPUT_FOLDER" "$GCS_OUTPUT_PATH"

    # Cleanup needs sudo because Docker creates files using its uid
    sudo rm -rf $TMPDIR
fi
