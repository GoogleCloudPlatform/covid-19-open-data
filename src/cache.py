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

import json
import warnings
import subprocess
from pathlib import Path
from datetime import datetime
from functools import partial
from argparse import ArgumentParser
from typing import Any, Callable, Dict, List

from lib.utils import ROOT


def parse_command(cmd: str) -> List[str]:
    if cmd == "curl":
        return ["python", str(ROOT / "src" / "cache" / "commands" / "curl_fetch.py")]
    if cmd == "static_fetch":
        return ["python", str(ROOT / "src" / "cache" / "commands" / "static_fetch.py")]
    if cmd == "dynamic_fetch":
        return ["node", str(ROOT / "src" / "cache" / "commands" / "dynamic_fetch.js")]
    if cmd.startswith("dynamic_custom/"):
        script_name = cmd.split("/")[-1]
        script_extension = script_name.split(".")[-1]
        assert script_extension == "js", "Dynamic script must be a NodeJS script"
        return ["node", str(ROOT / "src" / "cache" / "commands" / "dynamic_custom" / script_name)]
    raise ValueError(f"Unknown command {cmd}")


def process_source(cwd: Path, error_handler: Callable[[str], None], data_source: Dict[str, Any]):
    """
    Use the appropriate download command for the given data source.
    """

    cmd_tokens = parse_command(data_source["cmd"])
    cmd_tokens += ["--output", str(cwd / data_source["output"])]
    for option, value in data_source.items():
        if not option in ("cmd", "output"):
            value = value if isinstance(value, str) else json.dumps(value)
            cmd_tokens += [f"--{option}", value]

    print(">", " ".join(cmd_tokens))
    process = subprocess.Popen(cmd_tokens, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Wait for process to finish and get err streams
    try:
        stdout, stderr = process.communicate(timeout=30)

        # Write error to our stderr output
        if stderr:
            error_handler(stderr.decode("UTF-8"))

        # If there's any output, pipe it through
        if stdout:
            print(stdout.decode("UTF-8"))

        # Verify that the return code is zero
        if process.returncode != 0:
            error_handler(f"Exit code: {process.returncode}")

    except Exception as exc:
        # Most likely a timeout, but catching all potential errors so we can proceed
        error_handler(getattr(exc, "message", str(exc)))


# Parse arguments
parser = ArgumentParser()
parser.add_argument("--continue-on-error", action="store_true", default=False)
args = parser.parse_args()


def error_handler(error_message: str):
    """ Define error handling behavior depending on arguments """
    if args.continue_on_error:
        warnings.warn(error_message)
    else:
        raise RuntimeError(error_message)


# Create the output folder for the nearest hour in UTC time
now = datetime.utcnow()
output_name = now.strftime("%Y-%m-%d-%H")
output_path = ROOT / "output" / "cache"
snapshot_path = output_path / output_name
snapshot_path.mkdir(parents=True, exist_ok=True)

# Iterate over each source and process it
map_func = partial(process_source, snapshot_path, error_handler)
for source in json.loads(open(ROOT / "src" / "cache" / "config.json", "r").read()):
    map_func(source)

# Build a "sitemap" of the cache output folder
sitemap: Dict[str, List[str]] = {}
for snapshot in output_path.iterdir():
    if snapshot.name.startswith(".") or snapshot.name == "sitemap.json":
        continue
    if snapshot.is_file():
        warnings.warn(f"Unexpected file seen in root of {snapshot}")
        continue
    for cached_file in snapshot.iterdir():
        if not cached_file.is_file():
            warnings.warn(f"Unexpected folder seen in directory {cached_file}")
            continue
        sitemap_key = cached_file.stem
        snapshot_list = sitemap.get(sitemap_key, [])
        snapshot_list.append(str(cached_file.relative_to(output_path)))
        sitemap[sitemap_key] = list(sorted(snapshot_list))

# Output the sitemap
with open(output_path / "sitemap.json", "w") as fd:
    json.dump(sitemap, fd)
