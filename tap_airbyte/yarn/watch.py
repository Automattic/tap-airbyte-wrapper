import json
import os
import time
import argparse
from time import sleep

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
        sleep(30)
        with open(file_path, 'r') as file:
            raise Exception(file_path, file.readlines())
            while is_airbyte_app_running(yarn_config, app_id):
                line = file.readline()
                if not line:  # If EOF, wait for more content
                    time.sleep(2)
                    continue
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

    print(
        '{"type":"CATALOG","catalog":{"streams":[{"name":"pokemon","json_schema":{"type":"object","$schema":"http://json-schema.org/draft-07/schema#","additionalProperties":true,"properties":{"abilities":{"type":["null","array"],"items":{"type":["null","object"],"additionalProperties":true,"properties":{"ability":{"type":["null","object"],"additionalProperties":true,"properties":{"name":{"type":["null","string"]},"url":{"type":["null","string"]}}},"is_hidden":{"type":["null","boolean"]},"slot":{"type":["null","integer"]}}}},"base_experience":{"type":["null","integer"]},"forms":{"type":["null","array"],"items":{"type":["null","object"],"additionalProperties":true,"properties":{"name":{"type":["null","string"]},"url":{"type":["null","string"]}}}},"game_indices":{"type":["null","array"],"items":{"type":["null","object"],"additionalProperties":true,"properties":{"version":{"type":["null","object"],"additionalProperties":true,"properties":{"name":{"type":["null","string"]},"url":{"type":["null","string"]}}},"game_index":{"type":["null","integer"]}}}},"height":{"type":["null","integer"]},"held_items":{"type":["null","array"],"items":{"type":["null","object"],"additionalProperties":true,"properties":{"item":{"type":["null","object"],"additionalProperties":true,"properties":{"name":{"type":["null","string"]},"url":{"type":["null","string"]}}},"version_details":{"type":["null","array"],"items":{"type":["null","object"],"additionalProperties":true,"properties":{"version":{"type":["null","object"],"additionalProperties":true,"properties":{"name":{"type":["null","string"]},"url":{"type":["null","string"]}}},"rarity":{"type":["null","integer"]}}}}}}},"id":{"type":["null","integer"]},"is_default":{"type":["null","boolean"]},"location_area_encounters":{"type":["null","string"]},"moves":{"type":["null","array"],"items":{"type":["null","object"],"additionalProperties":true,"properties":{"move":{"type":["null","object"],"additionalProperties":true,"properties":{"name":{"type":["null","string"]},"url":{"type":["null","string"]}}},"version_group_details":{"type":["null","array"],"items":{"type":["null","object"],"additionalProperties":true,"properties":{"level_learned_at":{"type":["null","integer"]},"move_learn_method":{"type":["null","object"],"additionalProperties":true,"properties":{"name":{"type":["null","string"]},"url":{"type":["null","string"]}}},"version_group":{"type":["null","object"],"additionalProperties":true,"properties":{"name":{"type":["null","string"]},"url":{"type":["null","string"]}}}}}}}}},"name":{"type":["null","string"]},"order":{"type":["null","integer"]},"past_types":{"type":["null","array"],"items":{"type":["null","object"],"additionalProperties":true,"properties":{"generation":{"type":["null","object"],"additionalProperties":true,"properties":{"name":{"type":["null","string"]},"url":{"type":["null","string"]}}},"types":{"type":["null","array"],"items":{"type":["null","object"],"additionalProperties":true,"properties":{"type":{"type":["null","object"],"additionalProperties":true,"properties":{"name":{"type":["null","string"]},"url":{"type":["null","string"]}}},"slot":{"type":["null","integer"]}}}}}}},"species":{"type":["null","object"],"additionalProperties":true,"properties":{"name":{"type":["null","string"]},"url":{"type":["null","string"]}}},"sprites":{"type":["null","object"],"additionalProperties":true,"properties":{"back_default":{"type":["null","string"]},"back_female":{"type":["null","string"]},"back_shiny":{"type":["null","string"]},"back_shiny_female":{"type":["null","string"]},"front_default":{"type":["null","string"]},"front_female":{"type":["null","string"]},"front_shiny":{"type":["null","string"]},"front_shiny_female":{"type":["null","string"]}}},"stats":{"type":["null","array"],"items":{"type":["null","object"],"additionalProperties":true,"properties":{"base_stat":{"type":["null","integer"]},"effort":{"type":["null","integer"]},"stat":{"type":["null","object"],"additionalProperties":true,"properties":{"name":{"type":["null","string"]},"url":{"type":["null","string"]}}}}}},"types":{"type":["null","array"],"items":{"type":["null","object"],"additionalProperties":true,"properties":{"type":{"type":["null","object"],"properties":{"name":{"type":["null","string"]},"url":{"type":["null","string"]}}},"slot":{"type":["null","integer"]}}}},"weight":{"type":["null","integer"]}}},"supported_sync_modes":["full_refresh"],"source_defined_primary_key":[["id"]],"is_resumable":false}]}}'
    )
    exit(0)

    stream_file(
        file_path=args.file_path,
        yarn_config=json.loads(args.yarn_config),
        app_id=args.app_id
    )


if __name__ == "__main__":
    main()