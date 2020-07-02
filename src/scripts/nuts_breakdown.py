#!/usr/bin/env python
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


import os
import sys
from argparse import ArgumentParser
import datacommons as dc

# Add our library utils to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from lib.io import read_file
from lib.utils import ROOT


# Parse arguments from the command line
argparser = ArgumentParser()
argparser.add_argument("country_code", type=str)
argparser.add_argument("--nuts-level", type=int, default=2)
argparser.add_argument("--dc-api-key", type=str, default=os.environ["DATACOMMONS_API_KEY"])
args = argparser.parse_args()

# Get the country name
aux = read_file(ROOT / "src" / "data" / "metadata.csv").set_index("key")
country_name = aux.loc[args.country_code, "country_name"]

# Convert 2-letter to 3-letter country code
iso_codes = read_file(ROOT / "src" / "data" / "country_codes.csv").set_index("key")
country_code_alpha_3 = iso_codes.loc[args.country_code, "3166-1-alpha-3"]

dc.set_api_key(args.dc_api_key)
country = "country/{}".format(country_code_alpha_3)
nuts_name = "EurostatNUTS{}".format(args.nuts_level)
regions = dc.get_places_in([country], nuts_name)[country]
names = dc.get_property_values(regions, "name")
for key, name in names.items():
    region_name = name[0]
    region_code = key.split("/")[-1][2:]
    print(
        (
            "{country_code}_{region_code},"
            "{country_code},"
            "{country_name},"
            "{region_code},"
            "{region_name},"
            ",,,0"
        ).format(
            **{
                "country_code": args.country_code,
                "region_code": region_code,
                "country_name": country_name,
                "region_name": region_name,
            }
        )
    )
