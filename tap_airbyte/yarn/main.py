import json
from datetime import datetime
from functools import cache
from time import sleep
from typing import TypedDict
from tenacity import retry, stop_after_delay, wait_fixed

import requests
from requests.auth import HTTPBasicAuth


class MissingConfigException(Exception):
    pass

YARN_APP_FAILED_STATES = {'FAILED', 'KILLED'}
YARN_APP_TERMINAL_STATES = {'FINISHED'} | YARN_APP_FAILED_STATES

class YarnConfig(TypedDict):
    base_url: str
    username: str
    password: str
    extra_headers: dict
    queue: str
    mount_source: str
    mount_target: str


class YarnApplicationInfo(TypedDict):
    id: str  # pylint: disable=invalid-name
    state: str
    finalStatus: str


@cache
def _create_session(yarn_config: YarnConfig):
    session = requests.Session()
    if not yarn_config:
        raise MissingConfigException("Missing required 'yarn_config' in config")
    session.auth = HTTPBasicAuth(yarn_config.get('username'), yarn_config.get('password'))
    session.headers.update({"Content-Type": "application/json"} | yarn_config.get('extra_headers', {}))
    return session

def run_yarn_service(config: dict, command: str) -> str:
    """
    Run a service on YARN with the given command and return the application id
    """
    yarn_config: YarnConfig = config.get('yarn_config')
    airbyte_image = config['airbyte_spec'].get('image')
    airbyte_tag = config['airbyte_spec'].get('tag', 'latest')
    service_config = {
      "name": f"airbyte-service-{airbyte_image.split('/')[-1]}:{airbyte_tag}-{datetime.now().strftime('%Y%m%d%H%M')}",
      "version": "1.0",
      "components" :
        [
          {
            "name": f"airbyte-container-{airbyte_image.split('/')[-1]}",
            "number_of_containers": 1,
            "restart_policy": "NEVER",
            "artifact": {
                "id": f"{airbyte_image}:{airbyte_tag}",
                "type": "DOCKER"
            },
            # Redirect the stdout to a file so it can be read by Meltano
            # config and catalog files should be place on the mounted volume
            "launch_command": f'"python main.py {command} > {yarn_config["mount_target"]}/stdout"',
            "resource": {
              "cpus": 2,
              "memory": "1024"
            },
            "configuration": {
                "env": {
                    "YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE": "true",
                    "YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS": f"{yarn_config['mount_source']}:{yarn_config['mount_target']}:rw"
                },
                "properties": {
                    "yarn.service.default-readiness-check.enabled": "false",
                    "yarn.service.container-state-report-as-service-state": "true",
                    "dns.check.enabled": "false",
                    "docker.network": "bridge"
                }
            }
          }
        ],
        "configuration": {
            "properties": {
                # Since meltano will start the service, the retries should be controlled by it
                "yarn.service.am-restart.max-attempts": 1,
                # No need to track the service events
                "yarn.dispatcher.drain-events.timeout": 0
            }
        },
        "queue": yarn_config.get('queue', 'default')
    }

    session = _create_session(config)
    url = f"{yarn_config.get('base_url')}/app/v1/services"
    response = session.post(url, data=json.dumps(service_config))
    response.raise_for_status()
    service_uri = response.json().get('uri')
    app_id = _get_yarn_service_app_id(config, service_uri)
    return app_id


def _get_yarn_service_app_id(config: dict, service_uri: str) -> str:
    """
    Get the application id of the given service
    """
    session = _create_session(config.get('yarn_config'))
    url = f"{config.get('yarn_config').get('base_url')}/app/{service_uri}"
    response = session.get(url)
    app_id = None
    while not app_id:
        app_id = response.json().get('id')
        if response.json().get('state', 'STOPPED') == 'STOPPED':
            raise Exception(f"Yarn Service stopped before start the application: {response.json()}")
        sleep(1) # control the requests
    return app_id


def is_yarn_app_terminated(yarn_app: YarnApplicationInfo) -> bool:
    return bool(yarn_app and yarn_app.get('state') in YARN_APP_TERMINAL_STATES)


def is_yarn_app_failed(yarn_app: YarnApplicationInfo) -> bool:
    return yarn_app.get('finalStatus') != 'SUCCEEDED'


@retry(reraise=True, stop=stop_after_delay(60), wait=wait_fixed(3))
def get_yarn_service_application_info(yarn_config: YarnConfig, app_id: str) -> YarnApplicationInfo:
    """
    Get the application info of the given service
    """
    session = _create_session(yarn_config)
    url = f"{yarn_config.get('base_url')}/ws/v1/cluster/apps/{app_id}"
    response = session.get(url)
    response.raise_for_status()
    return response.json()