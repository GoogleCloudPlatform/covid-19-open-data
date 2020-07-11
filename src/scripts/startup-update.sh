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

#######################################
# Install the latest version of Python using pyenv
#######################################
function install_python {
    sudo apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev \
        libreadline-dev libsqlite3-dev llvm libncurses5-dev libncursesw5-dev xz-utils \
        tk-dev libffi-dev liblzma-dev python-openssl
    git clone https://github.com/pyenv/pyenv.git $HOME/.pyenv
    $HOME/.pyenv/plugins/python-build/bin/python-build 3.8.3 $HOME/python
}

# Delete ourselves after a one hour timeout
$(sleep 3600 && self_destruct)&

# Declare the branch to use to run code
readonly BRANCH=appengine

# Determine action from the metadata
readonly ACTION=$(read_metadata action)

# Install dependencies using the package manager
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -yq
sudo apt-get install -yq git wget curl

# Clone the repo into a temporary directory
readonly TMPDIR=$(mktemp -d -t opencovid-$(date +%Y-%m-%d-%H-%M-%S)-XXXX)
git clone https://github.com/open-covid-19/data.git --single-branch -b $BRANCH "$TMPDIR/opencovid"

# Install Python and its dependencies
install_python
$HOME/python/bin/python3.8 -m pip install -r "$TMPDIR/opencovid/src/requirements.txt"

if [ $ACTION == "deploy" ]
then
    # Re-deploy app and update the cron jobs
    $HOME/python/bin/python3.8 "$TMPDIR/opencovid/src/scripts/generate_cron.py" > "$TMPDIR/opencovid/src/cron.yaml"
    gcloud --quiet app deploy "$TMPDIR/opencovid/src/app.yaml"
    gcloud --quiet app deploy "$TMPDIR/opencovid/src/cron.yaml"

elif [ $ACTION == "publish" ]
then
    # Run the publish command
    $HOME/python/bin/python3.8 "$TMPDIR/opencovid/src/appengine.py" --command publish

else
    echo "No action provided"
fi

# Turn off this instance
self_destruct
