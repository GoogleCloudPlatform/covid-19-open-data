# Copyright 2021 Google LLC
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
import sys
from unittest import main
from .profiled_test_case import ProfiledTestCase

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from appengine import app


class TestAppEngine(ProfiledTestCase):
    def get_client(self):
        return app.test_client()

    def test_status_check(self):
        client = self.get_client()
        res = client.get("/status_check")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.get_data(as_text=True), "OK")


if __name__ == "__main__":
    sys.exit(main())
