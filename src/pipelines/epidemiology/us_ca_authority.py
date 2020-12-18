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

from functools import partial
from typing import Any, Dict
from bs4 import Tag
from pandas import DataFrame
from lib.data_source import DataSource
from lib.cached_data_source import CachedDataSource
from lib.cast import safe_int_cast
from lib.concurrent import process_map
from lib.io import count_html_tables, read_html
from lib.utils import table_rename


def _html_cell_parser(cell: Tag, row_idx: int, col_idx: int):
    # Replace zero-width whitespace present in tables
    return cell.get_text().strip().replace(bytes("\u200b", encoding="utf8").decode(), "")


def _extract_tables(html_content) -> DataFrame:
    selector = "table.ms-rteTable-4"

    # Tables keep changing order, so iterate through all until one looks good
    table_count = count_html_tables(html_content, selector=selector)

    for table_index in range(table_count):
        yield read_html(
            html_content,
            header=True,
            selector=selector,
            parser=_html_cell_parser,
            table_index=table_index,
        )


def _process_html_file(file_map: Dict[str, str], date: str) -> Dict[str, Any]:
    records = []

    fname = file_map[date]
    with open(fname, "r") as fd:
        html_content = fd.read()

    # Make sure the tables are exactly what we expect
    age_groups = ["0-17", "18-34", "35-49", "50-64", "65-79", "80-"]
    tables = list(_extract_tables(html_content))[-6:]
    if len(tables) == len(age_groups):
        for age_group, table in zip(age_groups, tables):
            total = table.iloc[-1]
            records.append(
                {
                    "key": "US_CA",
                    "date": date,
                    "age": age_group,
                    "total_confirmed": safe_int_cast(total["No. Cases"]),
                    "total_deceased": safe_int_cast(total["No. Deaths"]),
                }
            )

    return records


class CaliforniaOpenDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: Dict[str, DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:
        data = table_rename(
            dataframes[0],
            {
                "date": "date",
                "age_group": "age",
                "totalpositive": "total_confirmed",
                "deaths": "total_deceased",
            },
            drop=True,
        )
        data["key"] = "US_CA"
        data["date"] = data["date"].str.slice(0, 10)
        data["age"] = data["age"].str.replace("+", "-")
        data["age"] = data["age"].str.replace("Unknown", "age_unknown")
        data["age"] = data["age"].str.replace("Missing", "age_unknown")
        data["age"] = data["age"].str.replace("65 and Older", "65-")
        return data


class CaliforniaCachedDataSource(CachedDataSource):
    def parse(self, sources: Dict[str, str], aux: Dict[str, DataFrame], **parse_opts) -> DataFrame:
        file_map = sources["US_CA-mortality-stratified"]
        map_func = partial(_process_html_file, file_map)
        map_opts = dict(desc="Processing Cache Files", total=len(file_map))
        records = sum(process_map(map_func, file_map.keys(), **map_opts), [])
        assert len(records) > 0, "No records were found"
        return DataFrame.from_records(records)
