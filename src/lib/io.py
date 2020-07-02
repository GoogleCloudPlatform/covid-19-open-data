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

import re
from io import StringIO
from pathlib import Path
from typing import Callable, List, Union

import pandas
from unidecode import unidecode
from pandas import DataFrame
from pandas.api.types import is_numeric_dtype
from bs4 import BeautifulSoup, Tag

from .cast import safe_int_cast


def fuzzy_text(text: str, remove_regex: str = r"[^a-z\s]", remove_spaces: bool = True):
    # TODO: handle bad inputs (like empty text)
    text = unidecode(str(text)).lower()
    for token in ("y", "and", "of"):
        text = re.sub(f" {token} ", " ", text)
    text = re.sub(remove_regex, "", text)
    text = re.sub(r"^region", "", text)
    text = re.sub(r"region$", "", text)
    text = re.sub(r"^borough", "", text)
    text = re.sub(r"borough$", "", text)
    text = re.sub(r"^province", "", text)
    text = re.sub(r"province$", "", text)
    text = re.sub(r"^department", "", text)
    text = re.sub(r"department$", "", text)
    text = re.sub(r"^district", "", text)
    text = re.sub(r"district$", "", text)
    text = re.sub(r"\s+", "" if remove_spaces else " ", text)
    return text.strip()


def read_file(path: Union[Path, str], **read_opts):
    ext = str(path).split(".")[-1]

    if ext == "csv":
        return pandas.read_csv(
            path, **{**{"keep_default_na": False, "na_values": ["", "N/A"]}, **read_opts}
        )
    elif ext == "json":
        return pandas.read_json(path, **read_opts)
    elif ext == "html":
        return read_html(open(path).read(), **read_opts)
    elif ext == "xls" or ext == "xlsx":
        return pandas.read_excel(
            path, **{**{"keep_default_na": False, "na_values": ["", "N/A"]}, **read_opts}
        )
    else:
        raise ValueError("Unrecognized extension: %s" % ext)


def _get_html_columns(row: Tag) -> List[Tag]:
    cols = []
    for elem in filter(lambda row: isinstance(row, Tag), row.children):
        cols += [elem] * (safe_int_cast(elem.attrs.get("colspan", 1)) or 1)
    return list(cols)


def _default_html_cell_parser(cell: Tag, row_idx: int, col_idx: int):
    return cell.get_text().strip()


def count_html_tables(html: str, selector: str = "table"):
    page = BeautifulSoup(html, "lxml")
    return len(page.select(selector))


def wiki_html_cell_parser(cell: Tag, row_idx: int, col_idx: int):
    return re.sub(r"\[.+\]", "", cell.get_text().strip())


def read_html(
    html: str,
    selector: str = "table",
    table_index: int = 0,
    skiprows: int = 0,
    header: bool = False,
    parser: Callable = None,
) -> DataFrame:
    """ Parse an HTML table into a DataFrame """
    parser = parser if parser is not None else _default_html_cell_parser

    # Fetch table and read its rows
    page = BeautifulSoup(html, "lxml")
    table = page.select(selector)[table_index]
    rows = [_get_html_columns(row) for row in table.find_all("tr")]

    # Adjust for rowspan > 1
    for idx_row, row in enumerate(rows):
        for idx_cell, cell in enumerate(row):
            rowspan = int(cell.attrs.get("rowspan", 1))
            cell.attrs["rowspan"] = 1  # reset to prevent cascading
            for offset in range(1, rowspan):
                rows[idx_row + offset].insert(idx_cell, cell)

    # Get text within table cells and build dataframe
    records = []
    for row_idx, row in enumerate(rows[skiprows:]):
        records.append([parser(elem, row_idx, col_idx) for col_idx, elem in enumerate(row)])
    data = DataFrame.from_records(records)

    # Parse header if requested
    if header:
        data.columns = data.iloc[0]
        data = data.drop(data.index[0])

    return data


def export_csv(data: DataFrame, path: Union[Path, str]) -> None:
    """ Exports a DataFrame to CSV using consistent options """
    # Make a copy of the data to avoid overwriting
    data = data.copy()

    # Convert Int64 to string representation to avoid scientific notation of big numbers
    for column in data.columns:
        if is_numeric_dtype(data[column]):
            values = data[column].dropna()
            if len(values) > 0 and max(values) > 1e8:
                try:
                    data[column] = data[column].apply(safe_int_cast).astype("Int64")
                except:
                    data[column] = data[column].astype(str).fillna("")

    # Output to a buffer first
    buffer = StringIO()
    # Since all large quantities use Int64, we can assume floats will not be represented using the
    # exponential notation that %G formatting uses for large numbers
    data.to_csv(buffer, index=False, float_format="%.8G")
    output = buffer.getvalue()

    # Workaround for Namibia's code, which is interpreted as NaN when read back
    output = re.sub(r"^NA,", '"NA",', output)
    output = re.sub(r",NA,", ',"NA",', output)
    output = re.sub(r"\nNA,", '\n"NA",', output)

    # Write the output to the provided file
    with open(path, "w") as fd:
        fd.write(output)
