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
"""Utility for reporting errors on Google Cloud Engine machines."""

from google.cloud import error_reporting


def report_exception(func):
  """Decorator for reporting exceptions on Google Cloud Engine machines."""

  def wrapper(*args, **kwargs):
    try:
      client = error_reporting.Client()
    except Exception:  # pylint: disable=broad-except
      return func(*args, **kwargs)
    else:
      try:
        return func(*args, **kwargs)
      except Exception:  # pylint: disable=broad-except
        client.report_exception()

  return wrapper
