from typing import TypedDict

import requests
from requests import Session
from requests.auth import HTTPBasicAuth


class YarnConfig(TypedDict):
    base_url: str
    username: str
    password: str
    extra_headers: dict
    queue: str


class YarnApplicationInfo(TypedDict):
    id: str  # pylint: disable=invalid-name
    state: str
    finalStatus: str


def create_session(yarn_config: YarnConfig) -> Session:
    session = requests.Session()
    session.auth = HTTPBasicAuth(yarn_config['username'], yarn_config['password'])
    # `or {}`: a declared-but-unset setting arrives from meltano as None, not missing.
    session.headers.update({"Content-Type": "application/json"} | (yarn_config.get('extra_headers') or {}))
    return session