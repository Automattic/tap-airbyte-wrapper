import json
import argparse
from time import sleep

from main import is_airbyte_app_running

def stream_file(file_path: str, yarn_config: dict, app_id: str) -> None:
    """
    Stream a file line by line until the callback function returns a value.
    """
    # Start from the beginning of the file
    position = 0
    while is_airbyte_app_running(yarn_config, app_id):
        position = read_file(file_path, position)
        sleep(1) # If EOF is reached, wait briefly and then reopen
    read_file(file_path, position) # Read the remaining lines


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