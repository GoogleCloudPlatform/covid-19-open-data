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
from multiprocessing.pool import Pool, ThreadPool
from os import getenv
from typing import Any, Callable, Dict, Iterable, Union

from pandas import DataFrame, Series
from tqdm.contrib import concurrent

from .constants import GLOBAL_DISABLE_PROGRESS


class _ProcessExecutor(Pool):
    def __init__(self, max_workers: int = None, **kwargs):
        super().__init__(processes=max_workers, **kwargs)

    def map(self, func, iterable, chunksize=None):
        return self.imap(func, iterable)


class _ThreadExecutor(ThreadPool):
    def __init__(self, max_workers: int = None, **kwargs):
        super().__init__(processes=max_workers, **kwargs)

    def map(self, func, iterable, chunksize=None):
        return self.imap(func, iterable)


def process_map(map_func: Callable, map_iter: Iterable[Any], **tqdm_kwargs):
    tqdm_kwargs = {**{"disable": getenv(GLOBAL_DISABLE_PROGRESS)}, **tqdm_kwargs}
    # pylint: disable=protected-access
    return concurrent._executor_map(_ProcessExecutor, map_func, map_iter, **tqdm_kwargs)


def thread_map(map_func: Callable, map_iter: Iterable[Any], **tqdm_kwargs):
    tqdm_kwargs = {**{"disable": getenv(GLOBAL_DISABLE_PROGRESS)}, **tqdm_kwargs}
    # pylint: disable=protected-access
    return concurrent._executor_map(_ThreadExecutor, map_func, map_iter, **tqdm_kwargs)


def parallel_apply(
    data: Union[DataFrame, Series], map_func: Callable, index: bool = False, **tqdm_kwargs
) -> Iterable[Any]:
    if isinstance(data, Series):
        map_iter = data.items() if index else data.values
    elif isinstance(data, DataFrame):
        map_iter = data.iterrows() if index else data.values
    else:
        raise TypeError(f"Expected Series or DataFrame, found {type(data)}")

    return thread_map(map_func, map_iter, total=len(data), **tqdm_kwargs)


def _parallel_column_func(
    data: DataFrame, map_func_dict: Dict[str, Callable], column: str
) -> Iterable[Any]:
    return data[column].apply(map_func_dict[column])


def parallel_column_process(
    data: DataFrame, map_func_dict: Dict[str, Callable], **tqdm_kwargs
) -> DataFrame:
    map_iter = list(map_func_dict.keys())
    map_func = partial(_parallel_column_func, data, map_func_dict)
    map_opts = {"total": len(map_iter), "disable": True, **tqdm_kwargs}

    data_out = DataFrame(index=data.index, columns=map_iter)
    for name, values in zip(map_iter, thread_map(map_func, map_iter, **map_opts)):
        data_out[name] = values

    return data_out
