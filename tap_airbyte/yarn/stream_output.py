import json
import argparse

from main import stream_file, delete_yarn_service


def main():
    parser = argparse.ArgumentParser(description="Stream a airbyte output file until the service is finished.")
    parser.add_argument("file_path", type=str, help="Path to the file to stream.")
    parser.add_argument("--yarn_config", type=str, required=True, help="Yarn configs.")
    parser.add_argument("--app_id", type=str, required=True,
                        help="Yarn application id to wait for.")
    parser.add_argument("--service_name", type=str, required=True,
                        help="Service name that will be used to delete the service after the streaming is done.")
    args = parser.parse_args()

    stream_file(
        file_path=args.file_path,
        yarn_config=json.loads(args.yarn_config),
        app_id=args.app_id
    )
    delete_yarn_service(json.loads(args.yarn_config), args.service_name)


if __name__ == "__main__":
    main()