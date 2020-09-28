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
import json
import logging
import os

from functools import lru_cache
from typing import Callable

from pandas import Series

# Based on recipe for structured logging
# https://docs.python.org/3/howto/logging-cookbook.html#implementing-structured-logging


class LogEncoder(json.JSONEncoder):
    # pylint: disable=method-hidden
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

    name: str
    """ Name of the logger, defaults to the class name. """

    logger: logging.Logger
    """ Instance of logger which will be used. Each ErrorLogger instance has its own Logger. """

    def __init__(self, name: str = None):

        # Default to the classname
        self.name = name or self.__class__.__name__

        # Create an instance of logger
        self.logger = logging.getLogger(self.name)

        # Read logging level from env variable, default to INFO
        logging_level = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
        }.get(os.getenv("LOG_LEVEL"), logging.INFO)
        self.logger.setLevel(logging_level)

        # Only add a handler if it does not already have one
        if not self.logger.hasHandlers():

            # Configure the handler to use our preferred logging format
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(message)s"))
            self.logger.addHandler(handler)

    def timestamp(self) -> str:
        return datetime.datetime.now().isoformat()[:24]

    def _log_msg(self, log_func: Callable, msg: str, **kwargs) -> None:
        log_func(
            StructuredMessage(
                msg,
                logname=self.name,
                timestamp=self.timestamp(),
                # TODO: consider whether we should keep classname or if logname is sufficient
                classname=self.__class__.__name__,
                **kwargs,
            )
        )

    def log_error(self, msg: str, **kwargs) -> None:
        self._log_msg(self.logger.error, msg, **kwargs)

    def log_warning(self, msg: str, **kwargs) -> None:
        self._log_msg(self.logger.warning, msg, **kwargs)

    def log_info(self, msg: str, **kwargs) -> None:
        self._log_msg(self.logger.info, msg, **kwargs)

    def log_debug(self, msg: str, **kwargs) -> None:
        self._log_msg(self.logger.debug, msg, **kwargs)
