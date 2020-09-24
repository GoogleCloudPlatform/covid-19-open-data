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

from typing import Dict

from pandas import DataFrame

from lib.constants import SRC
from lib.data_source import DataSource
from lib.io import read_file
from lib.utils import table_merge


class EurostatDataSource(DataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts):
        # Read all files in the eurostat folder and merge them together
        eurostat_directory = SRC / "data" / "eurostat"
        dataframes = [read_file(file_name) for file_name in eurostat_directory.glob("*.csv")]
        data = table_merge(dataframes, how="outer").dropna(subset=["key"])

        # Use only keys available in metadata
        return data.merge(aux["metadata"][["key"]], how="inner")
