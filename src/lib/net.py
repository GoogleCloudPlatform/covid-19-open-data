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


import uuid
from pathlib import Path
from typing import BinaryIO

import requests
from .io import pbar


def download_snapshot(
    url: str, output_folder: Path, ext: str = None, skip_existing: bool = False, **download_opts
) -> str:
    """
    This function downloads a file into the snapshots folder and outputs the
    hashed file name based on the input URL. This is used to ensure
    reproducibility in downstream processing, which will not require network
    access.

    Args:
        url: URL to download a resource from
        output_folder: Root folder where snapshot, intermediate and tables will be placed.
        ext: Force extension when creating output file, handy when it cannot be guessed from URL.
        skip_existing: If true, skip download and simply return the deterministic path where this
            file would have been downloaded. If the file does not exist, this flag is ignored.
        download_opts: Keyword arguments passed to the `download` function.

    Returns:
        str: Absolute path where this file was downloaded. This is a deterministic output; the same
            URL will always produce the same output path.
    """

    # Create the snapshots folder if it does not exist
    (output_folder / "snapshot").mkdir(parents=True, exist_ok=True)

    # Create a deterministic file name
    if ext is None:
        ext = url.split(".")[-1]
    file_path = output_folder / "snapshot" / ("%s.%s" % (uuid.uuid5(uuid.NAMESPACE_DNS, url), ext))

    # Only download the file if skip_existing flag is not present
    # The skip_existing flag is ignored if the file does not already exist
    if not skip_existing or not file_path.exists():
        with open(file_path, "wb") as file_handle:
            download(url, file_handle, **download_opts)

    # Output the downloaded file path
    return str(file_path.absolute())


def download(
    url: str, file_handle: BinaryIO, progress: bool = False, spoof_browser: bool = True
) -> None:
    """
    Based on https://stackoverflow.com/a/37573701. It downloads the contents from the provided URL
    and writes them into a writeable binary stream.

    Args:
        url: The endpoint where contents are to be downloaded from
        file_handle: Writeable stream to write contents to
        progress: Display progress during the download using the lib.utils.pbar function
        spoof_browser: Pretend to be a web browser by adding user agent string to headers
    """
    headers = {"User-Agent": "Safari"} if spoof_browser else {}
    request_options = {"url": url, "headers": headers, "allow_redirects": True}
    if not progress:
        req = requests.get(**request_options)
        req.raise_for_status()
        file_handle.write(req.content)
    else:
        block_size = 1024
        req = requests.get(**request_options, stream=True)
        req.raise_for_status()
        total_size = int(req.headers.get("content-length", 0))
        progress_bar = pbar(total=total_size, unit="iB", unit_scale=True)
        for data in req.iter_content(block_size):
            progress_bar.update(len(data))
            file_handle.write(data)
        progress_bar.close()
