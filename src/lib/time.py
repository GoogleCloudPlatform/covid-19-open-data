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

import datetime
from .cast import safe_datetime_parse


def datetime_isoformat(value: str, date_format: str) -> str:
    date = safe_datetime_parse(value, date_format)
    if date is not None:
        return date.date().isoformat()
    else:
        return None


def date_offset(value: str, offset: int) -> str:
    assert offset is not None, "Offset none: %r" % offset
    date_value = datetime.date.fromisoformat(value)
    date_value += datetime.timedelta(days=offset)
    return date_value.isoformat()


def timezone_adjust(timestamp: str, offset: int) -> str:
    """ Adjust hour difference between a timezone and given offset """
    date_timestamp = datetime.datetime.fromisoformat(timestamp)
    if date_timestamp.hour <= 24 - offset:
        return date_timestamp.date().isoformat()
    else:
        return (date_timestamp + datetime.timedelta(days=1)).date().isoformat()
