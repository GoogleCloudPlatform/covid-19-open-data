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

from typing import Any, Callable, Iterable
from tqdm.contrib import concurrent
from multiprocess.pool import Pool, ThreadPool


class _ProcessExecutor(Pool):
    def __init__(self, max_workers: int = None, **kwargs):
        super().__init__(processes=max_workers, **kwargs)

    def map(self, map_func: Callable, map_iter: Iterable[Any]):
        return self.imap(map_func, map_iter)


class _ThreadExecutor(ThreadPool):
    def __init__(self, max_workers: int = None, **kwargs):
        super().__init__(processes=max_workers, **kwargs)

    def map(self, map_func: Callable, map_iter: Iterable[Any]):
        return self.imap(map_func, map_iter)


def process_map(map_func: Callable, map_iter: Iterable[Any], **tqdm_kwargs):
    return concurrent._executor_map(_ProcessExecutor, map_func, map_iter, **tqdm_kwargs)


def thread_map(map_func: Callable, map_iter: Iterable[Any], **tqdm_kwargs):
    return concurrent._executor_map(_ThreadExecutor, map_func, map_iter, **tqdm_kwargs)
