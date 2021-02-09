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
import uuid
from pathlib import Path
from typing import Any, BinaryIO, Dict, List, Union

import requests
from .concurrent import thread_map
from .error_logger import ErrorLogger
from .io import pbar, open_file_like
from .time import date_today


def download_snapshot(
    url: str,
    output_folder: Path,
    ext: str = None,
    skip_existing: bool = False,
    ignore_failure: bool = False,
    date_format: str = None,
    logger: ErrorLogger = ErrorLogger(),
    **download_opts,
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
        ignore_failure: If true, return `None` instead of raising an exception in case of failure.
        date_format: Use the provided date format and treat the URL as a template to try different
            dates in decreasing order until one works.
        logger: ErrorLogger instance to use for logging of events.
        ignore_failure: If true, return `None` instead of raising an exception in case of failure.
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

    # If we are told to skip download and the file already exist, early exit
    if skip_existing and file_path.exists():
        return str(file_path.absolute())

    # When a date format is given, it means the URL is a template
    elif date_format is not None:
        return _download_snapshot_try_date(
            url,
            file_path,
            ignore_failure=ignore_failure,
            date_format=date_format,
            logger=logger,
            **download_opts,
        )

    # If we don't have a date format, it means we don't need to manipulate the URL
    else:
        return _download_snapshot_simple(
            url, file_path, ignore_failure=ignore_failure, logger=logger, **download_opts
        )


def _download_snapshot_simple(
    url: str,
    file_path: Path,
    ignore_failure: bool = False,
    logger: ErrorLogger = ErrorLogger(),
    **download_opts,
) -> str:
    """See: download_snapshot for argument descriptions."""
    with open(file_path, "wb") as file_handle:
        try:
            logger.log_info(f"Downloading {url}")
            download(url, file_handle, **download_opts)
        except Exception as exc:
            # In case of failure, delete the file
            file_path.unlink()
            if ignore_failure:
                return None
            else:
                raise exc

    # Output the downloaded file path
    return str(file_path.absolute())


def _download_snapshot_try_date(
    url: str,
    file_path: Path,
    ignore_failure: bool = False,
    date_format: str = None,
    logger: ErrorLogger = ErrorLogger(),
    **download_opts,
) -> str:
    """
    Same as `_download_snapshot_simple` but trying to replace {date} in the URL with dates between
    today and 2020-01-01 until one works.
    """
    min_date = datetime.date.fromisoformat("2020-01-01")
    cur_date = datetime.date.fromisoformat(date_today(offset=1))
    while cur_date >= min_date:
        try:
            date_str = cur_date.strftime(date_format)
            return _download_snapshot_simple(
                url.format(date=date_str),
                file_path,
                ignore_failure=ignore_failure,
                logger=logger,
                **download_opts,
            )
        except requests.exceptions.HTTPError:
            cur_date -= datetime.timedelta(days=1)

    if ignore_failure:
        return None
    else:
        raise RuntimeError(f"No working URL found: {url}")


def download(
    url: str,
    file_handle: BinaryIO,
    progress: bool = False,
    spoof_browser: bool = True,
    timeout: int = 60,
    data: Dict[str, Any] = None,
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
    with open_file_like(file_handle, "wb") as fd:
        headers = {"User-Agent": "Safari"} if spoof_browser else {}
        request_options = {
            "url": url,
            "headers": headers,
            "allow_redirects": True,
            "timeout": timeout,
            "data": data,
        }
        if not progress:
            req = requests.get(**request_options)
            req.raise_for_status()
            fd.write(req.content)
        else:
            block_size = 1024
            req = requests.get(**request_options, stream=True)
            req.raise_for_status()
            total_size = int(req.headers.get("content-length", 0))
            progress_bar = pbar(total=total_size, unit="iB", unit_scale=True)
            for data in req.iter_content(block_size):
                progress_bar.update(len(data))
                fd.write(data)
            progress_bar.close()


def parallel_download(
    url_list: List[str], path_list: List[Union[str, Path]], **download_opts
) -> None:
    def _download_idx(idx: int) -> None:
        download(url_list[idx], path_list[idx], **download_opts)

    assert len(url_list) == len(path_list)
    return thread_map(_download_idx, range(len(url_list)))
