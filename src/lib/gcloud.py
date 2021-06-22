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

import os
import subprocess
from uuid import uuid4
from google.cloud import storage
from google.cloud.storage.blob import Blob
from google.oauth2.credentials import Credentials

from lib.constants import (
    GCE_IMAGE_ID,
    GCE_IMAGE_PROJECT,
    GCE_INSTANCE_TYPE,
    GCP_ENV_PROJECT,
    GCP_ENV_TOKEN,
    GCLOUD_BIN,
    GCP_ZONE,
)


def get_storage_client(gcp_project: str = None) -> storage.Client:
    """
    Creates an instance of google.cloud.storage.Client using a token if provided via env variable,
    otherwise the default credentials are used.
    """
    gcp_token = os.getenv(GCP_ENV_TOKEN)
    gcp_project = gcp_project or os.getenv(GCP_ENV_PROJECT)

    client_opts = {}
    if gcp_token is not None:
        client_opts["credentials"] = Credentials(gcp_token)
    if gcp_project is not None:
        client_opts["project"] = gcp_project

    return storage.Client(**client_opts)


def get_storage_bucket(bucket_name: str, gcp_project: str = None) -> storage.Bucket:
    """
    Gets an instance of the storage bucket for the specified bucket name.
    """
    client = get_storage_client(gcp_project)
    assert bucket_name is not None
    return client.bucket(bucket_name)


def start_instance_from_image(
    image_id: str,
    instance_type: str = GCE_INSTANCE_TYPE,
    service_account: str = None,
    zone: str = GCP_ZONE,
    preemptible: bool = True,
    startup_script: str = None,
) -> str:
    # Instance ID must start with a letter
    instance_id = "x" + str(uuid4())

    gcloud_args = [
        f"beta",
        f"compute",
        f"instances",
        f"create-with-container",
        f"{instance_id}",
        f"--zone={zone}",
        f"--machine-type={instance_type}",
        f"--scopes=https://www.googleapis.com/auth/cloud-platform",
        f"--tags=http-server",
        f"--image={GCE_IMAGE_ID}",
        f"--image-project={GCE_IMAGE_PROJECT}",
        f"--container-image={image_id}",
        f"--container-restart-policy=always",
        f"--container-env=PORT=80",
        f"--boot-disk-size=64GB",
        f"--boot-disk-type=pd-standard",
        f"--boot-disk-device-name={instance_id}",
        f"--quiet",
    ]

    if service_account:
        gcloud_args += [f"--service-account={service_account}"]

    if preemptible:
        gcloud_args += ["--preemptible"]

    if startup_script:
        gcloud_args += [f"--metadata-from-file=startup-script={startup_script}"]

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


def download_file(bucket_name: str, remote_path: str, local_path: str) -> None:
    """ Downloads a single file from the given GCS remote location into a local path """
    bucket = get_storage_bucket(bucket_name)
    return bucket.blob(remote_path).download_to_filename(str(local_path))
