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

# This script runs the fetching, processing, and copy-to-GCS scripts.
#!/bin/bash

## These must be run on the processor machine.

# apt-get install -y libabsl-dev python3-pandas python3-xlrd python3-yaml python3-pip git
# pip3 install absl-py
# git pull <dm_c19_modelling-england_data-repo>

set -e

export PYTHONPATH=$PYTHONPATH:${HOME}

LOCAL_DATA_DIR=/tmp/dm_c19_data

mkdir -p ${LOCAL_DATA_DIR}
cd ${HOME}/dm_c19_modelling/england_data
bash fetch_and_process_data.sh -o ${LOCAL_DATA_DIR}
bash copy_to_gcs.sh -o ${LOCAL_DATA_DIR}
rm -rf ${LOCAL_DATA_DIR}
