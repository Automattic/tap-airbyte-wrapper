"""Follow the Airbyte stdout file on HDFS and relay it to local stdout."""
import posixpath
import sys
from time import sleep, time
import logging

from tap_airbyte.yarn.service import (
    get_yarn_service_application_info,
    is_airbyte_app_running,
    is_yarn_app_failed,
    is_yarn_app_terminated,
)
from tap_airbyte.yarn.webhdfs import hdfs_delete, hdfs_file_length, hdfs_read_file

logger = logging.getLogger(__name__)


class TimeoutException(Exception):
    pass


def wait_for_file(file_path, yarn_config, app_id, timeout=300, interval=10):
    """
    Wait for a non-empty file to appear on HDFS, bailing out early if the
    YARN app fails before the file shows up.
    """
    start_time = time()
    while time() - start_time < timeout:
        if (hdfs_file_length(yarn_config, file_path) or 0) > 0:
            return # File created and not empty
        # Bail out immediately if the YARN app failed; otherwise keep
        # polling for the file (the app may have finished cleanly but the
        # helper's final commit can take a moment to land on HDFS).
        app_info = get_yarn_service_application_info(yarn_config, app_id)
        if is_yarn_app_terminated(app_info) and is_yarn_app_failed(app_info):
            raise Exception(f"Yarn application {app_id} failed.")
        sleep(interval)
    raise TimeoutException(f"File not created after {timeout}: {file_path}")


def read_file(yarn_config: dict, file_path: str, position: int) -> int:
    """
    Read the HDFS file from the given byte offset, print the complete lines,
    and return the new offset. A missing or shorter-than-position file
    (transient state mid `-put -f` commit) means "no new data yet".
    """
    length = hdfs_file_length(yarn_config, file_path)
    if length is None or length <= position:
        return position
    data = hdfs_read_file(yarn_config, file_path, offset=position)
    last_newline = data.rfind(b"\n")
    if last_newline == -1:
        return position # Incomplete line — wait until it's finished
    complete_lines = data[:last_newline + 1]
    sys.stdout.buffer.write(complete_lines)
    sys.stdout.buffer.flush()
    return position + len(complete_lines)


def stream_file(file_path: str, yarn_config: dict, app_id: str) -> None:
    """
    Stream an HDFS file line by line until the YARN application finishes,
    then clean up the per-run HDFS scratch dir.
    """
    hdfs_runtime_dir = posixpath.dirname(file_path)
    position = 0 # Start from the beginning of the file
    try:
        while is_airbyte_app_running(yarn_config, app_id):
            position = read_file(yarn_config, file_path, position)
            sleep(1) # If EOF is reached, wait briefly and then retry
        sleep(5) # Wait for the helper's final commit to land
        read_file(yarn_config, file_path, position) # Read the remaining lines
    except BaseException:
        # Keep the stdout file around for debugging a failed run, but drop
        # the files that can hold credentials.
        for name in ("config.json", "catalog.json", "state.json"):
            hdfs_delete(yarn_config, posixpath.join(hdfs_runtime_dir, name))
        raise
    else:
        hdfs_delete(yarn_config, hdfs_runtime_dir, recursive=True)