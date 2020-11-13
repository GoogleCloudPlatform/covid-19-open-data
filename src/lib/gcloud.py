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

import subprocess
from uuid import uuid4

from lib.constants import GCE_INSTANCE_TYPE, GCLOUD_BIN, GCP_ZONE, SRC


def start_instance_from_image(
    image_id: str,
    instance_type: str = GCE_INSTANCE_TYPE,
    service_account: str = None,
    zone: str = GCP_ZONE,
    preemptible: bool = True,
) -> str:
    # Instance ID must start with a letter
    instance_id = "x" + str(uuid4())

    gcloud_args = [
        f"beta",
        f"compute",
        f"instances",
        f"create",
        f"{instance_id}",
        f"--zone={zone}",
        f"--machine-type={instance_type}",
        f"--scopes=https://www.googleapis.com/auth/cloud-platform",
        f"--tags=http-server",
        f"--image={image_id}",
        f"--boot-disk-size=32GB",
        f"--boot-disk-type=pd-standard",
        f"--boot-disk-device-name={instance_id}",
        f"--metadata-from-file=startup-script={SRC / 'scripts' / 'startup-script-run.sh'}",
        f"--quiet",
    ]

    if service_account:
        gcloud_args += [f"--service-account={service_account}"]

    if preemptible:
        gcloud_args += ["--preemptible"]

    subprocess.check_call([GCLOUD_BIN] + gcloud_args)
    return instance_id


def delete_instance(instance_id: str, zone: str = GCP_ZONE) -> None:
    gcloud_args = [
        f"beta",
        f"compute",
        f"instances",
        f"delete",
        f"{instance_id}",
        f"--zone={zone}",
        f"--quiet",
    ]
    subprocess.check_call([GCLOUD_BIN] + gcloud_args)


def _get_instance_data(instance_id: str, format_data: str, zone: str = GCP_ZONE) -> str:
    gcloud_args = [
        f"compute",
        f"instances",
        f"describe",
        f"{instance_id}",
        f"--zone={zone}",
        f"--format={format_data}",
    ]
    return subprocess.check_output([GCLOUD_BIN] + gcloud_args).decode("UTF-8").strip()


def get_external_ip(instance_id: str, zone: str = GCP_ZONE) -> str:
    return _get_instance_data(
        instance_id=instance_id,
        format_data="get(networkInterfaces[0].accessConfigs[0].natIP)",
        zone=zone,
    )


def get_internal_ip(instance_id: str, zone: str = GCP_ZONE) -> str:
    return _get_instance_data(
        instance_id=instance_id, format_data="get(networkInterfaces[0].networkIP)", zone=zone
    )
