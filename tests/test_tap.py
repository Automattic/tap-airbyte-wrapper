from unittest.mock import patch

import orjson
import pytest

from tap_airbyte.tap import TapAirbyte
from tap_airbyte.yarn.streaming import TimeoutException


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


def _yarn_tap(timeout=None):
    tap = TapAirbyte.__new__(TapAirbyte)
    tap._config = {
        "airbyte_spec": {"image": "airbyte/source-pokeapi", "tag": "0.2.14"},
        "yarn_service_config": {
            "base_url": "https://gateway.example.com",
            "username": "u",
            "password": "p",
            "timeout": timeout,
        },
    }
    return tap


def test_to_yarn_command_shares_one_startup_deadline():
    """Time spent waiting for the app to start comes out of the first-output wait."""
    tap = _yarn_tap(timeout=100)
    with patch("tap_airbyte.tap.time") as mock_time, \
            patch("tap_airbyte.tap.run_yarn_service", return_value=("app-123", "/tmp/out")) as mock_run, \
            patch("tap_airbyte.tap.wait_for_file", return_value=None) as mock_wait:
        mock_time.monotonic.side_effect = [0, 40]  # 40s spent getting the app started
        tap._to_yarn_command("spec", runtime_tmp_dir="/tmp/runtime")

    assert mock_run.call_args.kwargs["timeout"] == 100
    assert mock_wait.call_args.kwargs["timeout"] == 60


def test_to_yarn_command_kills_app_when_first_output_times_out():
    tap = _yarn_tap()
    with patch("tap_airbyte.tap.run_yarn_service", return_value=("app-123", "/tmp/out")), \
            patch("tap_airbyte.tap.wait_for_file", side_effect=TimeoutException("File not created")), \
            patch("tap_airbyte.tap.kill_yarn_app") as mock_kill:
        with pytest.raises(TimeoutException):
            tap._to_yarn_command("spec", runtime_tmp_dir="/tmp/runtime")

    mock_kill.assert_called_once_with(tap.config["yarn_service_config"], "app-123")
