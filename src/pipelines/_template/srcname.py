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

from typing import Dict, List
from pandas import DataFrame
from lib.pipeline import DataSource


class SourceNameDataSource(DataSource):
    """ This is a custom pipeline that downloads a CSV file and outputs it as-is """

    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        """
        If the data fetched uses a supported format, (XLS, CSV or JSON), it will be automatically
        parsed and passed as an argument to this function. For data that requires special parsing,
        override the [DataSource.parse] function instead.
        """

        # We only fetched one source, so the list will be of length one
        data = dataframes[0]

        # Here we can manipulate the data any way we want...

        # The default merge strategy is defined in [DataSource.merge], see that method for
        # more details. Whenever possible, add a 'key' column to each record with a value found in
        # aux['metadata']['key'] for the best performance.
        # data['key'] = ...

        # Finally, return the data which is ready to go to the next step of the pipeline
        return data

    ##
    # Any functions of DataSource could be overridden here. We could also
    # define our own functions to the class if necessary.
    ##
