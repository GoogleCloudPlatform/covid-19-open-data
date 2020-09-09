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

readonly NAME=$(curl -q -X GET http://metadata.google.internal/computeMetadata/v1/instance/name -H 'Metadata-Flavor: Google')
readonly ZONE=$(curl -q -X GET http://metadata.google.internal/computeMetadata/v1/instance/zone -H 'Metadata-Flavor: Google')

#######################################
# Self-deletes this instance
#######################################
function self_destruct {
    gcloud --quiet compute instances delete $NAME --zone=$ZONE
}

#######################################
# Reads metadata associated with this running instance
#######################################
function read_metadata {
    gcloud compute instances describe $NAME --zone=$ZONE --flatten="metadata[$1]"  | tail -n 1 | sed 's/^\s*//g'
}

# Delete ourselves after a two hour timeout
$(sleep 7200 && self_destruct)&

# Declare the branch to use to run code
readonly BRANCH=main

# Determine action from the metadata
readonly ACTION=$(read_metadata action)

# Install dependencies using the package manager
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -yq && sleep 5
sudo apt-get install -yq git wget curl python3.8 python3.8-dev python3-pip

# Clone the repo into a temporary directory
readonly TMPDIR=$(mktemp -d -t opencovid-$(date +%Y-%m-%d-%H-%M-%S)-XXXX)
git clone https://github.com/GoogleCloudPlatform/covid-19-open-data.git --single-branch -b $BRANCH "$TMPDIR/opencovid"

# Install Python and its dependencies
python3.8 -m pip install -r "$TMPDIR/opencovid/src/requirements.txt"

# Allow for Python to bind to port 80
sudo setcap CAP_NET_BIND_SERVICE=+eip $(which python3.8)

if [ $ACTION == "null" ]
then
    # If no action is given, start server
    python3.8 "$TMPDIR/opencovid/src/appengine.py" --command server

else
    # Run the provided command
    python3.8 "$TMPDIR/opencovid/src/appengine.py" --command $ACTION
fi

# Turn off this instance after action is finished
self_destruct
