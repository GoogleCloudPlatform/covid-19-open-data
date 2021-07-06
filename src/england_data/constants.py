# pylint: disable=g-bad-file-header
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
"""Constants used in standardization of the England dataset."""

import enum

REGION_PREFIX = "reg_"
OBSERVATION_PREFIX = "obs_"


@enum.unique
class Columns(enum.Enum):
  """Standard column names."""

  DATE = "date"
  OBSERVATION_TYPE = "observation_type"
  REG_NHSER_NAME = REGION_PREFIX + "nhser_name"
  REG_CCG_NAME = REGION_PREFIX + "ccg_name"
  REG_CCG_CODE = REGION_PREFIX + "ccg_code"
  REG_UTLA_NAME = REGION_PREFIX + "utla_name"
  REG_UTLA_CODE = REGION_PREFIX + "utla_code"
  REG_LTLA_NAME = REGION_PREFIX + "ltla_name"
  REG_LTLA_CODE = REGION_PREFIX + "ltla_code"
  REG_SUB_REGION_1 = REGION_PREFIX + "sub_region_1"
  REG_SUB_REGION_2 = REGION_PREFIX + "sub_region_2"
  REG_ISO_3166_2_CODE = REGION_PREFIX + "iso_3166_2_code"
  REG_TRUST_CODE = REGION_PREFIX + "trust_code"
  REG_TRUST_NAME = REGION_PREFIX + "trust_name"
  OBS_DEATHS = OBSERVATION_PREFIX + "deaths"
  OBS_CASES = OBSERVATION_PREFIX + "cases"
  OBS_ONLINE_111_F_0 = OBSERVATION_PREFIX + "online_111_f_0"
  OBS_ONLINE_111_F_19 = OBSERVATION_PREFIX + "online_111_f_19"
  OBS_ONLINE_111_F_70 = OBSERVATION_PREFIX + "online_111_f_70"
  OBS_ONLINE_111_F_U = OBSERVATION_PREFIX + "online_111_f_u"
  OBS_ONLINE_111_M_0 = OBSERVATION_PREFIX + "online_111_m_0"
  OBS_ONLINE_111_M_19 = OBSERVATION_PREFIX + "online_111_m_19"
  OBS_ONLINE_111_M_70 = OBSERVATION_PREFIX + "online_111_m_70"
  OBS_ONLINE_111_M_U = OBSERVATION_PREFIX + "online_111_m_u"
  OBS_ONLINE_111_U_0 = OBSERVATION_PREFIX + "online_111_u_0"
  OBS_ONLINE_111_U_19 = OBSERVATION_PREFIX + "online_111_u_19"
  OBS_ONLINE_111_U_70 = OBSERVATION_PREFIX + "online_111_u_70"
  OBS_ONLINE_111_U_U = OBSERVATION_PREFIX + "online_111_u_u"
  OBS_CALL_111_F_0 = OBSERVATION_PREFIX + "calls_111_f_0"
  OBS_CALL_111_F_19 = OBSERVATION_PREFIX + "calls_111_f_19"
  OBS_CALL_111_F_70 = OBSERVATION_PREFIX + "calls_111_f_70"
  OBS_CALL_111_F_U = OBSERVATION_PREFIX + "calls_111_f_u"
  OBS_CALL_111_M_0 = OBSERVATION_PREFIX + "calls_111_m_0"
  OBS_CALL_111_M_19 = OBSERVATION_PREFIX + "calls_111_m_19"
  OBS_CALL_111_M_70 = OBSERVATION_PREFIX + "calls_111_m_70"
  OBS_CALL_111_M_U = OBSERVATION_PREFIX + "calls_111_m_u"
  OBS_CALL_111_U_0 = OBSERVATION_PREFIX + "calls_111_u_0"
  OBS_CALL_111_U_19 = OBSERVATION_PREFIX + "calls_111_u_19"
  OBS_CALL_111_U_70 = OBSERVATION_PREFIX + "calls_111_u_70"
  OBS_CALL_111_U_U = OBSERVATION_PREFIX + "calls_111_u_u"
  OBS_CALL_999_F_0 = OBSERVATION_PREFIX + "calls_999_f_0"
  OBS_CALL_999_F_19 = OBSERVATION_PREFIX + "calls_999_f_19"
  OBS_CALL_999_F_70 = OBSERVATION_PREFIX + "calls_999_f_70"
  OBS_CALL_999_F_U = OBSERVATION_PREFIX + "calls_999_f_u"
  OBS_CALL_999_M_0 = OBSERVATION_PREFIX + "calls_999_m_0"
  OBS_CALL_999_M_19 = OBSERVATION_PREFIX + "calls_999_m_19"
  OBS_CALL_999_M_70 = OBSERVATION_PREFIX + "calls_999_m_70"
  OBS_CALL_999_M_U = OBSERVATION_PREFIX + "calls_999_m_u"
  OBS_CALL_999_U_0 = OBSERVATION_PREFIX + "calls_999_u_0"
  OBS_CALL_999_U_19 = OBSERVATION_PREFIX + "calls_999_u_19"
  OBS_CALL_999_U_70 = OBSERVATION_PREFIX + "calls_999_u_70"
  OBS_CALL_999_U_U = OBSERVATION_PREFIX + "calls_999_u_u"
  OBS_POPULATION_GENDER_AGE = OBSERVATION_PREFIX + "population_{gender}_{age:02d}"


@enum.unique
class DataTypes(enum.Enum):
  """Different data types available in the England dataset."""
  DAILY_DEATHS = "daily_deaths"
  DAILY_CASES = "daily_cases"
  ONLINE_111_AND_CALLS_111_999 = "online_111_and_calls_111_999"
  POPULATION = "population"
