# Copyright 2021 Google LLC
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

from pandas import DataFrame


def country_subregion1s(metadata: DataFrame, country_code: str) -> DataFrame:
    query = '(country_code == "{}") & subregion1_code.notna() & subregion2_code.isna() & locality_code.isna()'.format(
        country_code)
    return metadata.query(query)


def country_subregion2s(metadata: DataFrame, country_code: str) -> DataFrame:
    query = '(country_code == "{}") & subregion2_code.notna() & locality_code.isna()'.format(country_code)
    return metadata.query(query)
