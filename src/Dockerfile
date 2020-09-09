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
#
FROM gcr.io/google-appengine/python

# Install Google Cloud SDK
ENV GCLOUD_VERSION 308.0.0
RUN wget --quiet "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${GCLOUD_VERSION}-linux-x86_64.tar.gz" -O gcloud.tar.gz && \
    tar -xf gcloud.tar.gz -C /opt && \
    /opt/google-cloud-sdk/install.sh --quiet && \
    rm gcloud.tar.gz

# Add the gcloud command to the PATH and update components
ENV PATH /opt/google-cloud-sdk/bin:$PATH
RUN gcloud components update --quiet
RUN gcloud components install beta --quiet

# Create virtualenv environment
RUN virtualenv /env -p python3
ENV VIRTUAL_ENV /env
ENV PATH /env/bin:$PATH

# Add application code and install dependencies
ADD . /app
RUN pip install -r /app/requirements.txt

# Start application server
CMD gunicorn -b :$PORT appengine:app --timeout 5400
