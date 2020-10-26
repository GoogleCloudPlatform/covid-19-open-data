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

# Declare the branch to use to run code
readonly BRANCH=main

# Install dependencies using the package manager
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -yq && sleep 5
sudo apt-get install -yq git wget curl python3.8 python3.8-dev python3-pip

# Update gcloud and add links to known location
mkdir -p /opt/google-cloud-sdk/bin
ln -s `which gcloud` /opt/google-cloud-sdk/bin/gcloud

# Clone the repo into app directory
readonly APPDIR=/opt/open-covid
git clone https://github.com/GoogleCloudPlatform/covid-19-open-data.git --single-branch -b "$BRANCH" "$APPDIR"

# Install Python and its dependencies
python3.8 -m pip install -r "$APPDIR/src/requirements.txt"

# Allow for Python to bind to port 80
sudo setcap CAP_NET_BIND_SERVICE=+eip $(which python3.8)

# Install service to run server listening for commands
cat <<EOF > /etc/systemd/system/open-covid.service
[Unit]
Description=gunicorn daemon
After=network.target

[Service]
WorkingDirectory=/opt/open-covid/src
ExecStart=/usr/local/bin/gunicorn -b :80 appengine:app --timeout 5400
ExecReload=/bin/kill -s HUP $MAINPID

[Install]
WantedBy=multi-user.target
EOF

# Enable and start the service
systemctl enable --now open-covid
