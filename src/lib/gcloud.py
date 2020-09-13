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

from lib.constants import GCS_CONTAINER_ID

# TODO(owahltinez): move configuration to external file
_gcloud_bin = "/opt/google-cloud-sdk/bin/gcloud"
_default_zone = "us-east1"
_default_instance_type = "n2-standard-4"
_host_image_id = "cos-stable-81-12871-1196-0"


def start_instance(
    instance_type: str = _default_instance_type,
    service_account: str = None,
    zone: str = _default_zone,
) -> str:
    # Instance ID must start with a letter
    instance_id = "x" + str(uuid4())

    gcloud_args = [
        f"beta",
        f"compute",
        f"instances",
        f"create-with-container",
        f"{instance_id}",
        f"--preemptible",
        f"--zone={zone}",
        f"--machine-type={instance_type}",
        f"--scopes=https://www.googleapis.com/auth/cloud-platform",
        f"--tags=http-server",
        f"--image={_host_image_id}",
        f"--image-project=cos-cloud",
        f"--boot-disk-size=10GB",
        f"--boot-disk-type=pd-standard",
        f"--boot-disk-device-name={instance_id}",
        f"--container-image={GCS_CONTAINER_ID}",
        f"--container-restart-policy=always",
        f"--container-env=PORT=80",
        f"--labels=container-vm={_host_image_id}",
        f"--quiet",
    ]

    if service_account:
        gcloud_args += [f"--service-account={service_account}"]

    subprocess.check_call([_gcloud_bin] + gcloud_args)
    return instance_id


def delete_instance(instance_id: str, zone: str = _default_zone) -> None:
    gcloud_args = [
        f"beta",
        f"compute",
        f"instances",
        f"delete",
        f"{instance_id}",
        f"--zone={zone}",
        f"--quiet",
    ]
    subprocess.check_call([_gcloud_bin] + gcloud_args)


def _get_instance_data(instance_id: str, format_data: str, zone: str = _default_zone) -> str:
    gcloud_args = [
        f"compute",
        f"instances",
        f"describe",
        f"{instance_id}",
        f"--zone={zone}",
        f"--format={format_data}",
    ]
    return subprocess.check_output([_gcloud_bin] + gcloud_args).decode("UTF-8").strip()


def get_external_ip(instance_id: str, zone: str = _default_zone) -> str:
    return _get_instance_data(
        instance_id=instance_id,
        format_data="get(networkInterfaces[0].accessConfigs[0].natIP)",
        zone=zone,
    )


def get_internal_ip(instance_id: str, zone: str = _default_zone) -> str:
    return _get_instance_data(
        instance_id=instance_id, format_data="get(networkInterfaces[0].networkIP)", zone=zone
    )
