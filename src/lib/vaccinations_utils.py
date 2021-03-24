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


def add_total_persons_vaccinated_estimate(data: DataFrame):
    """
    Assuming fully and partially vaccinated persons have 2 and 1 doses respectively,
    total_persons_partially_vaccinated = total_vaccine_doses_administered - 2 * total_persons_fully_vaccinated
    Therefore, total_persons_vaccinated = total_persons_partially_vaccinated + total_persons_fully_vaccinated
    = total_vaccine_doses_administered - total_persons_fully_vaccinated
    """
    data["total_persons_vaccinated"] = data["total_vaccine_doses_administered"] - data["total_persons_fully_vaccinated"]
