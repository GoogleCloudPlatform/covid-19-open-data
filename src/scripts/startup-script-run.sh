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
