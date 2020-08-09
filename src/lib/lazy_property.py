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

from typing import Callable

LAZY_PROPERTY_PREFIX = "_lazy_"


def lazy_property(func: Callable) -> Callable:
    """
    Decorator that makes a property lazy-evaluated. Inspired by:
    https://stevenloria.com/lazy-properties.

    Arguments:
        func: The function to be decorated as a lazy @property.
    Returns:
        Callable: The decorated function.
    """
    attr_name = LAZY_PROPERTY_PREFIX + func.__name__

    @property
    def _lazy_property(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, func(self))
        return getattr(self, attr_name)

    return _lazy_property
