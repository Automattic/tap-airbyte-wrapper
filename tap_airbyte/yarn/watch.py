import json
import time
import argparse

from tap_airbyte.yarn.main import get_yarn_service_application_info, is_yarn_app_terminated, is_yarn_app_failed


def stream_file(file_path: str, yarn_config: dict, app_id: str) -> None:
    """
    Stream a file line by line until the callback function returns a value.
    """
    with open(file_path, 'r') as file:
        # Move to the end of the file for streaming new content
        file.seek(0, 2)
        while True:
            line = file.readline()
            if not line:  # If at EOF, wait for more content
                time.sleep(1)
                app_info = get_yarn_service_application_info(yarn_config, app_id)
                if is_yarn_app_terminated(app_info):
                    if is_yarn_app_failed(app_info):
                        raise Exception(f"Yarn application {app_id} failed.")
                    exit(0)
                continue

            print(line, end='')


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