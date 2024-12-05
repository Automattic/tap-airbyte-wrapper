import json
import os
import time
import argparse

from main import get_yarn_service_application_info, is_yarn_app_terminated, is_yarn_app_failed

def wait_for_file(file_path, timeout=60, interval=1):
    """
    Waits for a file to be created within a specified timeout.

    :param file_path: Path to the file to wait for.
    :param timeout: Maximum time to wait for the file, in seconds.
    :param interval: Time between checks, in seconds.
    :return: True if the file is created, False if the timeout is reached.
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        if os.path.exists(file_path):
            return True # File found
        time.sleep(interval)
    return False  # Timeout reached

def stream_file(file_path: str, yarn_config: dict, app_id: str) -> None:
    """
    Stream a file line by line until the callback function returns a value.
    """
    if wait_for_file(file_path):
        with open(file_path, 'r') as file:
            while is_airbyte_app_running(yarn_config, app_id):
                where = file.tell()
                line = file.readline()
                if not line:  # If EOF, wait for more content
                    time.sleep(2)
                    file.seek(where)
                    continue
                raise Exception(line)
                print(line, end='')
            # Read remaining lines after the service is finished
            for line in file:
                print(line, end='')
    else:
        raise Exception(f"File not found: {file_path}")

def is_airbyte_app_running(yarn_config: dict, app_id: str) -> bool:
    app_info = get_yarn_service_application_info(yarn_config, app_id)
    if is_yarn_app_terminated(app_info):
        if is_yarn_app_failed(app_info):
            raise Exception(f"Yarn application {app_id} failed.")
        return False # Yarn application finished successfully
    return True


def main():
    parser = argparse.ArgumentParser(description="Stream a airbyte output file until the service is finished.")
    parser.add_argument("file_path", type=str, help="Path to the file to stream.")
    parser.add_argument("--yarn_config", type=str, required=True, help="Yarn configs.")
    parser.add_argument("--app_id", type=str, required=True,
                        help="Yarn application id to wait for.")
    args = parser.parse_args()

    stream_file(
        file_path=args.file_path,
        yarn_config=json.loads(args.yarn_config),
        app_id=args.app_id
    )


if __name__ == "__main__":
    main()