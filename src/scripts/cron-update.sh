#!/bin/sh

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

set -xe

# Clone the repo into a temporary directory
TMPDIR=$(mktemp -d -t opencovid-$(date +%Y-%m-%d-%H-%M-%S)-XXXX)
git clone https://github.com/open-covid-19/data.git --single-branch -b main "$TMPDIR/opencovid"

# Download the intermediate files into the working directory
mkdir -p "$TMPDIR/opencovid/output/snapshot"
mkdir -p "$TMPDIR/opencovid/output/intermediate"
gsutil -m cp -r gs://covid19-open-data/snapshot "$TMPDIR/opencovid/output/"
gsutil -m cp -r gs://covid19-open-data/intermediate "$TMPDIR/opencovid/output/"

# Install dependencies and run the update command in a Docker instance
docker run -v "$TMPDIR/opencovid":/opencovid -w /opencovid -i python:3.8-buster /bin/bash -s <<EOF
cd src
# Install locales
apt-get update && apt-get install -yq tzdata locales
sh -c 'echo en_US.UTF-8 UTF-8 >> /etc/locale.gen'
sh -c 'echo es_ES.UTF-8 UTF-8 >> /etc/locale.gen'
locale-gen en_US.UTF-8
locale-gen es_ES.UTF-8
dpkg-reconfigure --frontend noninteractive locales
export LC_ALL="en_US.UTF-8"
export LC_CTYPE="en_US.UTF-8"
# Install Python dependencies
pip install -r requirements.txt
# Update geography first, since lat-lon values are used in other pipelines
python3 update.py --only geography
# Update all the other data pipelines
python3 update.py --exclude geography
# Get the files ready for publishing
python3 publish.py
EOF

# Upload the outputs to Google Cloud
gsutil -m cp -r "$TMPDIR/opencovid/output/snapshot" gs://covid19-open-data/
gsutil -m cp -r "$TMPDIR/opencovid/output/intermediate" gs://covid19-open-data/
gsutil -m cp -r "$TMPDIR/opencovid/output/public/v2" gs://covid19-open-data/

# Cleanup
sudo rm -rf $TMPDIR
