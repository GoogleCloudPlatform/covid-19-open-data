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

from concurrent.futures import ProcessPoolExecutor as Pool
from concurrent.futures import ThreadPoolExecutor as ThreadPool
from functools import partial
from multiprocessing import cpu_count, get_context
from typing import Any, Callable, Dict, Iterable, Type, Union

from pandas import DataFrame, Series

from .io import pbar


def _get_pool(pool_type: Type, max_workers: int) -> Pool:
    if pool_type == ThreadPool:
        pool = ThreadPool(max_workers)
        setattr(pool, "imap", pool.map)
        return pool
    elif pool_type == Pool:
        pool = Pool(max_workers, mp_context=get_context("spawn"))
        setattr(pool, "imap", pool.map)
        return pool
    else:
        raise TypeError(f"Unknown pool type: {pool_type}")


def _parallel_map(
    pool_type: Type, map_func: Callable, map_iter: Iterable[Any], **tqdm_kwargs
) -> Iterable[Any]:
    chunk_size = tqdm_kwargs.pop("chunk_size", 1)
    max_workers = tqdm_kwargs.pop("max_workers", min(32, cpu_count() + 4))
    total = tqdm_kwargs.pop("total", len(map_iter) if hasattr(map_iter, "__len__") else None)
    progress_bar = pbar(total=total, **tqdm_kwargs)
    with _get_pool(pool_type, max_workers) as pool:
        for result in pool.imap(map_func, map_iter, chunksize=chunk_size):
            progress_bar.update(1)
            yield result
    progress_bar.close()


def process_map(map_func: Callable, map_iter: Iterable[Any], **tqdm_kwargs):
    return _parallel_map(Pool, map_func, map_iter, **tqdm_kwargs)


def thread_map(map_func: Callable, map_iter: Iterable[Any], **tqdm_kwargs):
    return _parallel_map(ThreadPool, map_func, map_iter, **tqdm_kwargs)


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
