import json
from datetime import datetime
from typing import TypedDict

import requests
from requests.auth import HTTPBasicAuth


class MissingConfigException(Exception):
    pass

class YarnConfig(TypedDict):
    base_url: str
    username: str
    password: str
    extra_headers: dict
    queue: str
    mount_src: str
    mount_dest: str

def _create_session(yarn_config: YarnConfig):
    session = requests.Session()
    if not yarn_config:
        raise MissingConfigException("Missing required 'yarn_config' in config")
    session.auth = HTTPBasicAuth(yarn_config.get('username'), yarn_config.get('password'))
    session.headers.update({"Content-Type": "application/json"} | yarn_config.get('extra_headers', {}))
    return session

def run_yarn_service(config: dict, command: str, catalog: dict = None):
    yarn_config: YarnConfig = config.get('yarn_config')
    airbyte_image = config['airbyte_spec'].get('image')
    airbyte_tag = config['airbyte_spec'].get('tag', 'latest')
    docker_mount = config.get('docker_mount')
    if not yarn_config or not docker_mount:
        raise MissingConfigException("Missing required 'yarn_config' or 'docker_mount' in config")
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
            "launch_command": command,
            "resource": {
              "cpus": 2,
              "memory": "1024"
            },
            "configuration": {
                "env": {
                    "YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE": "true",
                    "YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS": f"{docker_mount.get('source')}:{docker_mount.get('target')}:rw"
                },
                "properties": {
                    "yarn.service.default-readiness-check.enabled": "false",
                    "yarn.service.container-state-report-as-service-state": "true",
                    "dns.check.enabled": "false",
                    "docker.network": "bridge"
                },
                "files": [
                    {
                        "type": "JSON",
                        "dest_file": "/tmp/config.json",
                        "properties": config.get("airbyte_config", {})
                    },
                    {
                        "type": "JSON",
                        "dest_file": "/tmp/catalog.json",
                        "properties": catalog or {}
                    }
                ]
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
    return response.json()
