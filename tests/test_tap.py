from unittest.mock import patch

import orjson

from tap_airbyte.tap import TapAirbyte


def test_to_yarn_command_uses_string_arguments():
    tap = TapAirbyte.__new__(TapAirbyte)
    tap._config = {
        "airbyte_spec": {"image": "airbyte/source-pokeapi", "tag": "0.2.14"},
        "yarn_service_config": {
            "base_url": "https://gateway.example.com",
            "username": "u",
            "password": "p",
        },
    }
    with patch("tap_airbyte.tap.run_yarn_service", return_value=("app-123", "/tmp/out")), \
            patch("tap_airbyte.tap.wait_for_file", return_value=None):
        command = tap._to_yarn_command("spec", runtime_tmp_dir="/tmp/runtime")

    assert all(isinstance(arg, str) for arg in command)
    assert orjson.loads(command[5]) == tap.config["yarn_service_config"]
