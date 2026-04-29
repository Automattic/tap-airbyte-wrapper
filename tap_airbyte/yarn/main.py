import os
import textwrap
from datetime import datetime
from time import sleep, time
from typing import TypedDict, Mapping, Any
import logging
import hashlib

from requests import Session
from tenacity import retry, stop_after_delay, wait_fixed

import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


YARN_APP_FAILED_STATES = {'FAILED', 'KILLED'}
YARN_APP_TERMINAL_STATES = {'FINISHED'} | YARN_APP_FAILED_STATES

# Helper executed inside the Airbyte container: appends main.py's stdout to
# a local-disk buffer file and periodically copies it to the FUSE output
# path via an atomic rename.
#
# fuse_dfs's fsync() is weakly implemented and HDFS append (O_APPEND) is
# not supported on this cluster (write returns EOPNOTSUPP). The only way
# to publish in-flight bytes is to write a complete file and close it.
# Doing it via temp-file + rename keeps readers from ever seeing a partial
# state — HDFS rename is atomic, and each new version is a strict superset
# of the previous one, so position-based incremental reads remain correct.
#
# Buffering on /tmp (container-local disk) instead of process memory keeps
# RAM bounded regardless of stream size, at the cost of local disk usage
# proportional to total output.
#
# 20s interval keeps NameNode RPC load down (each commit copies the full
# buffer + 1 rename) while still giving readers timely visibility.
_FSYNC_HELPER = textwrap.dedent("""
    import os, sys, time, shutil
    path = sys.argv[1]
    tmp = path + ".tmp"
    local = "/tmp/airbyte_buf"
    buf_fd = os.open(local, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    last_commit = time.monotonic()

    def commit():
        shutil.copyfile(local, tmp)
        os.rename(tmp, path)

    try:
        for line in sys.stdin.buffer:
            os.write(buf_fd, line)
            if time.monotonic() - last_commit >= 20:
                commit()
                last_commit = time.monotonic()
    finally:
        os.close(buf_fd)
        commit()
""").strip()

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
    output_file = f'stdout-{main_command}'
    output_file_path = os.path.join(runtime_tmp_dir, output_file)
    stderr_file_path = os.path.join(runtime_tmp_dir, "stderr")
    helper_path = os.path.join(runtime_tmp_dir, "helper.py")
    launch_script_path = os.path.join(runtime_tmp_dir, "launch.sh")
    # Write helper.py and launch.sh to the shared FUSE mount and have YARN
    # run `sh launch.sh`. Use a FIFO (not a pipe) so we can capture and
    # exit with main.py's status via `$?` — bash-only `${PIPESTATUS[0]}`
    # gets misparsed by dash as "Invalid argument number". The helper
    # lives in its own file to sidestep multi-line `python -c '...'`
    # quoting concerns.
    with open(helper_path, "w") as f:
        f.write(_FSYNC_HELPER + "\n")
    launch_script = textwrap.dedent(f"""\
        #!/bin/sh
        {{
          # Named pipe in container-local /tmp
          mkfifo /tmp/airbyte_pipe
          # Helper drains the FIFO and writes to the FUSE output file,
          # closing+reopening every 20s so HDFS commits in-flight bytes.
          python -u {helper_path} {output_file_path} </tmp/airbyte_pipe &
          HELPER_PID=$!
          python -u main.py {command} >/tmp/airbyte_pipe
          # Capture main.py's exit status; wait for helper to flush; exit
          # with main.py's status so YARN reflects Airbyte's success/fail.
          EC=$?
          wait $HELPER_PID
          exit $EC
        }} 2>{stderr_file_path}
    """)
    with open(launch_script_path, "w") as f:
        f.write(launch_script)
    # +x so the wrapper image's `sh -c` entrypoint can exec the path
    # directly (kernel honors the shebang). launch_command is just the
    # path — no spaces, no tokenization worry.
    os.chmod(launch_script_path, 0o755)
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
            # All launch logic (FIFO between main.py and the fsync helper,
            # exit-code capture, stderr capture) lives in launch.sh — see
            # above. The wrapper image's entrypoint is `["/bin/sh", "-c"]`,
            # so passing just the script path lets sh -c exec it directly
            # and the kernel honors the shebang. config and catalog files
            # should be place on the mounted volume.
            "launch_command": f'"{launch_script_path}"',
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


def kill_yarn_app(yarn_config: dict, app_id: str) -> None:
    session = _create_session(yarn_config)
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


class TimeoutException(Exception):
    pass


def wait_for_file(file_path, yarn_config, app_id, timeout=300, interval=1):
    """
    Waits for a file to be created within a specified timeout.

    Bails out early if the YARN app terminates before the file appears,
    instead of polling the (never-created) file for the full timeout.

    :param file_path: Path to the file to wait for.
    :param yarn_config: YARN config used to poll application status.
    :param app_id: YARN application id used to poll application status.
    :param timeout: Maximum time to wait for the file, in seconds.
    :param interval: Time between checks, in seconds.
    :return: True if the file is created, False if the timeout is reached.
    """
    start_time = time()
    while time() - start_time < timeout:
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            return # File created and not empty
        app_info = get_yarn_service_application_info(yarn_config, app_id)
        if is_yarn_app_terminated(app_info):
            # Only raise if the YARN app actually failed; a successful
            # run with no output is legitimate (e.g., empty stream) — let
            # stream_file handle the missing/empty file downstream.
            if is_yarn_app_failed(app_info):
                raise Exception(f"Yarn application {app_id} failed.")
            return
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
            # Only print if line ends with newline (full line written)
            if line.endswith('\n'):
                print(line, end='', flush=True)
                position = file.tell()
            else:
                # Incomplete line — wait until it's finished
                return position


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
