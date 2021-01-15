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
from typing import Iterable
from .cast import safe_datetime_parse

ISO_DATE_FORMAT = "%Y-%m-%d"


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


def date_range(start: str, end: str) -> Iterable[str]:
    """
    Range of dates from `start` to `end`, both inclusive.

    Arguments:
        start: Start date in ISO format YYYY-MM-DD.
        end: Start date in ISO format YYYY-MM-DD.
    Returns:
        Iterable[str]: Iterable of dates from `start` to `end`.
    """
    start_date = datetime.datetime.strptime(start, ISO_DATE_FORMAT)
    end_date = datetime.datetime.strptime(end, ISO_DATE_FORMAT)
    assert start_date <= end_date, f"Start date must be less or equal than end date"
    for idx in range((end_date - start_date).days + 1):
        yield (start_date + datetime.timedelta(days=idx)).strftime(ISO_DATE_FORMAT)


def date_today(offset: int = 0) -> str:
    """ Returns today's date for UTC timezone in ISO format. """
    date_value = datetime.datetime.utcnow() + datetime.timedelta(days=offset)
    return date_value.date().isoformat()
