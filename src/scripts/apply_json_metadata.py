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


from google.cloud import storage
import sys

def apply_json_metadata(bucket_name, prefix_name):
    """
    Applies Content-Type and gzip Content-Encoding to json files in a bucket prefix

    In order to allow for decompressive transcoding and serving of gzipped assets to clients
    who can decompress themselves, both the content type and content encoding meta data need
    to be set on JSON objects.  Most methods of transferring objects into a bucket do not
    correctly set this meta data, so we have this utility to correct for this after the fact.

    See also: https://cloud.google.com/storage/docs/transcoding
    """

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    for blob in bucket.list_blobs(prefix=prefix_name):
        if(blob.name.endswith("json")):
            print(blob.name)
            if(blob.content_type != "application/json" or 
                    blob.content_encoding != "gzip" or
                    blob.content_disposition != "inline"):
                blob.content_type = "application/json"
                blob.content_encoding = "gzip"
                blob.content_disposition = "inline"
                blob.patch()

if __name__ == "__main__":
    if(len(sys.argv) != 3):
        print("Usage: apply_json_meta [bucket_name] [prefix_name]")
    else:
        apply_json_metadata(sys.argv[1],sys.argv[2])
