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
import warnings
from typing import List
import json
import logging
from functools import lru_cache
from pandas import Series

# Based on recipe for structured logging
# https://docs.python.org/3/howto/logging-cookbook.html#implementing-structured-logging


class LogEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, set):
            return tuple(o)
        elif isinstance(o, str):
            return o.encode("unicode_escape").decode("ascii")
        elif isinstance(o, Series):
            return o.to_dict()
        elif isinstance(o, Exception):
            return f"{o.__class__.__name__}: {str(o)}"
        return super(LogEncoder, self).default(o)


class StructuredMessage:
    def __init__(self, message, **kwargs):
        self._kwargs = kwargs
        self._kwargs["message"] = message

    @lru_cache()
    def __str__(self):
        return LogEncoder().encode(self._kwargs)


class ErrorLogger:
    """
    Simple class to be inherited by other classes to add error logging functions.
    """

    logging.basicConfig(format="%(message)s")

    def timestamp(self) -> str:
        return datetime.datetime.now().isoformat()[:24]

    def errlog(self, msg: str, **kwargs) -> None:
        logging.warning(
            StructuredMessage(
                msg, timestamp=self.timestamp(), classname=self.__class__.__name__, **kwargs
            )
        )
