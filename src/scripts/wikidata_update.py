#!/usr/bin/env python
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

"""
Script used to update entities on Wikidata, inspired by:
https://github.com/HeardLibrary/digital-scholarship/blob/master/code/wikibase/api/write-statements.py.
"""

import time
from getpass import getpass
from argparse import ArgumentParser
from typing import Iterable, Tuple

import requests
from tqdm import tqdm


def write_statement(
    session: requests.Session, endpoint: str, token: str, triple: Tuple[str, str, str]
) -> None:
    res = session.post(
        endpoint,
        data={
            "action": "wbcreateclaim",
            "format": "json",
            "entity": triple[0],
            "snaktype": "value",
            "token": token,
            "property": triple[1],
            "value": triple[2],
        },
    ).json()
    assert res.get("success"), f"Write statement failed: {res}"


def read_input_data(fname: str) -> Iterable[Tuple[str, str, str]]:
    """ Read comma-separated triples of <subject, property, object> from file """
    with open(fname, "r") as fh:
        for line in fh:
            if not line or line == "" or line.startswith("#"):
                continue
            triple = line.strip().split(",")
            assert len(triple) == 3, f"Malformed line in input file: {line}"
            yield triple


def get_token(session: requests.Session, endpoint: str, username: str, password: str) -> str:
    """
    Getting a token which can be used to issue write statements is a multistep process:
        1. Obtain a login token by logging in (this method might be deprecated soon)
        2. Use the login token to issue the log-in command and retrieve a session token
        3. Use the session token to issue a CSRF token request
    """
    session_token = (
        session.get(
            url=endpoint,
            params={"action": "query", "meta": "tokens", "type": "login", "format": "json"},
        )
        .json()
        .get("query", {})
        .get("tokens", {})
        .get("logintoken")
    )

    login_result = session.post(
        url=endpoint,
        data={
            "action": "login",
            "lgname": username,
            "lgpassword": password,
            "lgtoken": session_token,
            "format": "json",
        },
    ).json()
    assert login_result.get("login", {}).get("result") == "Success", f"Login failed: {login_result}"

    csrf_token = (
        session.get(url=endpoint, params={"action": "query", "meta": "tokens", "format": "json"})
        .json()
        .get("query", {})
        .get("tokens", {})
        .get("csrftoken")
    )
    assert csrf_token is not None, "Error retrieving CSRF token"

    return csrf_token


if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("--input", type=str, required=True)
    argparser.add_argument("--username", type=str, required=True)
    argparser.add_argument("--password", type=str, default=None)
    argparser.add_argument("--test", action="store_true")
    args = argparser.parse_args()

    triples = list(read_input_data(args.input))
    assert len(triples) > 0, "Input data file is empty"

    username = args.username
    password = args.password or getpass(prompt="Password:", stream=None)
    endpoint = f"https://{'test' if args.test else 'www'}.wikidata.org/w/api.php"

    max_retries = 5
    session = requests.Session()
    token = get_token(session, endpoint, username, password)
    for triple in tqdm(triples, desc="Writing triples"):
        wait_time = 8
        for i in range(max_retries):
            try:
                write_statement(session, endpoint, token, triple)
                time.sleep(wait_time)
            except Exception as exc:
                if i + 1 == max_retries:
                    raise exc
                else:
                    wait_time *= 2
