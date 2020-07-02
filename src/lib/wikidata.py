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

import sys
from typing import Any, Dict
import requests
from lib.cast import safe_float_cast


def _all_property_keys(props: Dict[str, str]):
    for key in props.keys():
        for part in key.split("|"):
            yield part


def _obj_get(obj, *keys):
    for key in keys:
        if isinstance(obj, list):
            obj = obj[key]
        else:
            obj = obj.get(key, {})
    return obj


def _cast_property_amount(value):
    return safe_float_cast((value or "").replace("+", ""))


def _process_property(obj, name: str, prop: str):
    # Attempt to sort by "point in time" P585
    value_array = list(
        sorted(
            _obj_get(obj, "claims", prop),
            key=lambda x: _obj_get(x, "qualifiers", "P585", 0, "datavalue", "value", "time") or "0",
        )
    )
    # Get the latest known value
    value = _obj_get(value_array, -1, "mainsnak", "datavalue", "value") if value_array else {}
    if "amount" in value:
        value = {name: _cast_property_amount(value.get("amount"))}

    # Some values do not have an "amount" envelope
    elif not isinstance(value, dict):
        value = {name: value}

    return value


def wikidata_properties(props: Dict[str, str], entity: str) -> Dict[str, Any]:
    api_base = "https://www.wikidata.org/w/api.php?action=wbgetclaims&format=json"
    res = requests.get("{}&entity={}".format(api_base, entity)).json()

    # Early exit: entity not found
    if not res.get("claims"):
        print("Request: {}&entity={}".format(api_base, entity), file=sys.stderr)
        print("Response: {}".format(res), file=sys.stderr)
        return {}

    # When props is empty, we return all properties
    output: Dict[str, Any] = {} if props else res["claims"]

    # Traverse the returned object and find the properties requested
    for name, prop in (props or {}).items():
        output = {**output, **_process_property(res, name, prop)}

    # Return the object with all properties found
    return output
