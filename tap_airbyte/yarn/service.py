"""YARN service API: submit the Airbyte container and track its status."""
import os
import posixpath
import textwrap
from datetime import datetime
from time import sleep
from typing import Mapping, Any
import logging
import hashlib

from tenacity import retry, stop_after_delay, wait_fixed

from tap_airbyte.yarn.session import YarnConfig, YarnApplicationInfo, create_session
from tap_airbyte.yarn.webhdfs import hdfs_mkdirs, hdfs_write_file

logger = logging.getLogger(__name__)


YARN_APP_FAILED_STATES = {'FAILED', 'KILLED'}
YARN_APP_TERMINAL_STATES = {'FINISHED'} | YARN_APP_FAILED_STATES

# Where YARN localizes the uploaded files (launch.sh, helper.py, config
# files) inside the Airbyte container.
CONTAINER_CONF_DIR = "/tmp/airbyte"

# Runs inside the Airbyte container: buffers main.py's stdout on local disk
# and uploads it to HDFS (argv[1]) via `hdfs dfs -put -f` every 20s (each
# invocation spins up a JVM, ~1-3s). The buffer is append-only, so the
# committed file's prefix is stable and byte offsets stay valid across
# commits; the Meltano side reads it with WebHDFS offset reads (`read_file`).
_HDFS_PUT_HELPER = textwrap.dedent("""
    import sys, time, subprocess
    hdfs_path = sys.argv[1]
    local = "/tmp/airbyte_buf"
    buf = open(local, "wb")
    last_commit = time.monotonic()

    def commit():
        buf.flush()
        subprocess.run(
            ["hdfs", "dfs", "-D", "fs.permissions.umask-mode=077",
             "-put", "-f", local, hdfs_path],
            check=True,
        )

    try:
        for line in sys.stdin.buffer:
            buf.write(line)
            if time.monotonic() - last_commit >= 20:
                commit()
                last_commit = time.monotonic()
    finally:
        commit()
        buf.close()
""").strip()


def run_yarn_service(config: Mapping[str, Any], command: str, runtime_tmp_dir: str) -> tuple[str, str]:
    """
    Run a service on YARN with the given command.

    Uploads everything the container needs (files staged in runtime_tmp_dir,
    plus the generated launch.sh and helper.py) to a per-run HDFS dir over
    WebHDFS; YARN localizes them into the container at CONTAINER_CONF_DIR.

    Returns the application id and the HDFS path of the stdout file.
    """
    yarn_config: YarnConfig = config['yarn_service_config']
    airbyte_image = config['airbyte_spec'].get('image')
    airbyte_tag = config['airbyte_spec'].get('tag', 'latest')
    main_command = command.split()[0].lstrip("--")
    output_file = f'stdout-{main_command}'
    # Per-run HDFS scratch dir. YARN has no cross-node per-app HDFS temp
    # dir (container workspaces are node-local), so stage under the
    # submitting user's home; stream_file removes it after a clean run.
    hdfs_base_path = os.getenv("HDFS_PATH") or f"/user/{yarn_config['username']}/.airbyte"
    hdfs_runtime_dir = posixpath.join(hdfs_base_path, os.path.basename(runtime_tmp_dir))
    hdfs_output_path = posixpath.join(hdfs_runtime_dir, output_file)
    # FIFO (not a pipe) so main.py's status is capturable via `$?` —
    # bash-only `${PIPESTATUS[0]}` gets misparsed by dash. Stderr flows to
    # the container's stderr so YARN log aggregation captures it.
    launch_script = textwrap.dedent(f"""\
        #!/bin/sh
        mkfifo /tmp/airbyte_pipe
        # Helper drains the FIFO and uploads to HDFS every 20s.
        python -u {CONTAINER_CONF_DIR}/helper.py {hdfs_output_path} </tmp/airbyte_pipe &
        HELPER_PID=$!
        python -u main.py {command} >/tmp/airbyte_pipe
        # Exit with main.py's status so YARN reflects Airbyte's success/fail.
        EC=$?
        wait $HELPER_PID
        exit $EC
    """)
    # Owner-only dir; the uploaded files default to 600 in hdfs_write_file.
    hdfs_mkdirs(yarn_config, hdfs_runtime_dir)
    hdfs_write_file(yarn_config, posixpath.join(hdfs_runtime_dir, "helper.py"), _HDFS_PUT_HELPER + "\n")
    hdfs_write_file(yarn_config, posixpath.join(hdfs_runtime_dir, "launch.sh"), launch_script)
    localized_files = ["helper.py", "launch.sh"]
    # Ship whatever the tap staged locally (config/catalog/state.json).
    for name in sorted(os.listdir(runtime_tmp_dir)):
        local_path = os.path.join(runtime_tmp_dir, name)
        if not os.path.isfile(local_path):
            continue
        with open(local_path, "rb") as f:
            hdfs_write_file(yarn_config, posixpath.join(hdfs_runtime_dir, name), f.read())
        localized_files.append(name)
    service_hash = hashlib.sha256(f"{datetime.now().strftime('%Y%m%d%H%M%S')}-{runtime_tmp_dir.split('/')[-1].split('-')[-1]}".encode()).hexdigest()
    service_name = f"{airbyte_image.split('/')[-1]}-{service_hash[:10]}"
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
            # The wrapper image's entrypoint is `["/bin/sh", "-c"]`; a bare
            # script path avoids quoting/tokenization issues in docker CMD.
            "launch_command": f"{CONTAINER_CONF_DIR}/launch.sh",
            # YARN pulls these from HDFS and mounts them into the docker
            # container at their absolute dest_file paths.
            "files": [
                {
                    "type": "STATIC",
                    "src_file": posixpath.join(hdfs_runtime_dir, name),
                    "dest_file": f"{CONTAINER_CONF_DIR}/{name}",
                }
                for name in localized_files
            ],
            "resource": {
              "cpus": 2,
              "memory": "1024"
            },
            "configuration": {
                "env": {
                    "YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE": "true",
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
    session = create_session(yarn_config)
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
    return app_id, hdfs_output_path


def _get_yarn_service_app_id(yarn_config: YarnConfig, service_uri: str) -> str:
    """
    Get the application id of a running service
    """
    session = create_session(yarn_config)
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
    session = create_session(yarn_config)
    url = f"{yarn_config.get('base_url')}/ws/v1/cluster/apps/{app_id}"
    response = session.get(url)
    response.raise_for_status()
    return response.json().get('app', {})


def kill_yarn_app(yarn_config: dict, app_id: str) -> None:
    session = create_session(yarn_config)
    url = f"{yarn_config.get('base_url')}/ws/v1/cluster/apps/{app_id}/state"
    response = session.put(url, json={"state": "KILLED"})
    response.raise_for_status()
    logger.info("Killed YARN application %s", app_id)


def is_airbyte_app_running(yarn_config: dict, app_id: str) -> bool:
    app_info = get_yarn_service_application_info(yarn_config, app_id)
    logger.info(app_info)
    if is_yarn_app_terminated(app_info):
        logger.info("TERMINATED")
        if is_yarn_app_failed(app_info):
            raise Exception(f"Yarn application {app_id} failed.")
        return False # Yarn application finished successfully
    return True