import os
from datetime import datetime
from time import sleep, time
from typing import TypedDict, Mapping, Any
import logging

from requests import Session
from tenacity import retry, stop_after_delay, wait_fixed

import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


YARN_APP_FAILED_STATES = {'FAILED', 'KILLED'}
YARN_APP_TERMINAL_STATES = {'FINISHED'} | YARN_APP_FAILED_STATES

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


def _create_session(yarn_config: YarnConfig) -> Session:
    session = requests.Session()
    session.auth = HTTPBasicAuth(yarn_config['username'], yarn_config['password'])
    session.headers.update({"Content-Type": "application/json"} | yarn_config.get('extra_headers', {}))
    return session

def run_yarn_service(config: Mapping[str, Any], command: str, runtime_tmp_dir: str) -> tuple[str, str]:
    """
    Run a service on YARN with the given command and return the application id
    """
    yarn_config: YarnConfig = config['yarn_service_config']
    airbyte_image = config['airbyte_spec'].get('image')
    airbyte_tag = config['airbyte_spec'].get('tag', 'latest')
    airbyte_mount_dir = os.getenv("AIRBYTE_MOUNT_DIR", "/tmp")
    main_command = command.split()[0].lstrip("--")
    output_file = f'stdout-{command.split()[0].lstrip("--")}'
    output_file_path = os.path.join(runtime_tmp_dir, output_file)
    service_name = f"{airbyte_image.split('/')[-1]}-{datetime.now().strftime('%Y%m%d%H%M%S')}-{runtime_tmp_dir.split('/')[-1]}"
    service_config = {
      "name": service_name,
      "version": "1.0",
      "components" :
        [
          {
            "name": main_command,
            "number_of_containers": 1,
            "restart_policy": "NEVER",
            "artifact": {
                "id": f"{airbyte_image}:{airbyte_tag}",
                "type": "DOCKER"
            },
            # Redirect the stdout to a file so it can be read by Meltano
            # config and catalog files should be place on the mounted volume
            "launch_command": f'"python main.py {command} > {output_file_path}"',
            "resource": {
              "cpus": 2,
              "memory": "1024"
            },
            "configuration": {
                "env": {
                    "YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE": "true",
                    "YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS": f"{airbyte_mount_dir}:{airbyte_mount_dir}:rw",
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
    session = _create_session(yarn_config)
    url = f"{yarn_config['base_url']}/app/v1/services"
    logger.debug('Creating YARN service %s...', service_name)
    logger.debug('Config: %s', service_config) # tests
    response = session.post(url, json=service_config)
    logger.info(response.json())
    response.raise_for_status()
    service_uri = response.json().get('uri')
    logger.debug('YARN service created with uri: %s', service_uri)
    app_id = _get_yarn_service_app_id(yarn_config, service_uri)
    logger.debug('YARN service running with app_id: %s', app_id)
    return app_id, output_file


def _get_yarn_service_app_id(yarn_config: YarnConfig, service_uri: str) -> str:
    """
    Get the application id of a running service
    """
    session = _create_session(yarn_config)
    url = f"{yarn_config.get('base_url')}/app/{service_uri}"
    app_id = None
    state = None
    logger.debug('Waiting for the application id...')
    while not app_id or state not in {'STARTED', 'SUCCEEDED'}:
        logger.debug(f'APP_ID: {app_id}, STATE: {state}')
        response = session.get(url)
        app_info = response.json()
        app_id = app_info.get('id')
        state = app_info.get('state', 'STOPPED')
        if state in {'STOPPED', 'FAILED'}:
            raise Exception(f"Yarn Service stopped/failed before start the application: {response.json()}")
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
    return response.json().get('app', {})


def is_airbyte_app_running(yarn_config: dict, app_id: str) -> bool:
    app_info = get_yarn_service_application_info(yarn_config, app_id)
    logger.info(app_info)
    if is_yarn_app_terminated(app_info):
        logger.info("TERMINATED")
        if is_yarn_app_failed(app_info):
            raise Exception(f"Yarn application {app_id} failed.")
        return False # Yarn application finished successfully
    return True


class TimeoutException(Exception):
    pass


def wait_for_file(file_path, timeout=300, interval=1):
    """
    Waits for a file to be created within a specified timeout.

    :param file_path: Path to the file to wait for.
    :param timeout: Maximum time to wait for the file, in seconds.
    :param interval: Time between checks, in seconds.
    :return: True if the file is created, False if the timeout is reached.
    """
    start_time = time()
    while time() - start_time < timeout:
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            return # File created and not empty
        sleep(interval)
    raise TimeoutException(f"File not created after {timeout}: {file_path}")


def read_file(file_path, position) -> int:
    """
    Read a file from a given position and print the content.
    """
    with open(file_path, 'r') as file:
        while True:
            file.seek(position)
            line = file.readline()
            if not line:
                return position
            print(line, end='')
            position = file.tell()  # Store current position


def stream_file(file_path: str, yarn_config: dict, app_id: str) -> None:
    """
    Stream a file line by line until the callback function returns a value.
    """
    position = 0 # Start from the beginning of the file
    while is_airbyte_app_running(yarn_config, app_id):
        position = read_file(file_path, position)
        sleep(1) # If EOF is reached, wait briefly and then reopen
    sleep(5) # Wait for the file to be completely written and synced
    read_file(file_path, position) # Read the remaining lines
