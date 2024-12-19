from tempfile import NamedTemporaryFile
from unittest.mock import patch

import pytest

from tap_airbyte.yarn.main import read_file, wait_for_file, TimeoutException, stream_file


@pytest.fixture(autouse=True)
def mock_sleep():
    # Ensure the patch target matches the location of `sleep` usage
    with patch("tap_airbyte.yarn.main.sleep", return_value=None) as mock:
        yield mock


@pytest.mark.parametrize(
    "start_position, expected_output, expected_position",
    [
        (0, "line1\nline2\nline3\n", len("line1\nline2\nline3\n")),  # Start from the beginning
        (6, "line2\nline3\n", len("line1\nline2\nline3\n")),         # Start from the middle
    ]
)
def test_read_file(capfd, start_position, expected_output, expected_position):
    file_content = "line1\nline2\nline3\n"
    with NamedTemporaryFile(mode="w+") as file:
        file.write(file_content)
        file.seek(0)
        position = read_file(file.name, start_position)

    # Assert the returned position is as expected
    assert position == expected_position

    # Capture and assert the output
    captured = capfd.readouterr()
    assert captured.out == expected_output


def test_wait_for_file_success():
    file_path = "/path/to/testfile"

    with patch("os.path.exists") as mock_exists:
        # Simulate the file being created on the second check
        mock_exists.side_effect = [False, False, True]

        # Call the method
        wait_for_file(file_path, timeout=10, interval=1)

        # Assert `os.path.exists` was called
        assert mock_exists.call_count == 3


def test_wait_for_file_timeout():
    file_path = "/path/to/nonexistentfile"

    with patch("os.path.exists", return_value=False):
        # Expect the function to raise a TimeoutException
        with pytest.raises(TimeoutException, match=f"File not created after 5: {file_path}"):
            wait_for_file(file_path, timeout=5, interval=1)


def test_stream_file(mock_sleep):
    file_path = "/path/to/testfile"
    yarn_config = {"key": "value"}
    app_id = "app_123"

    # Mock `is_airbyte_app_running` to return True twice, then False
    with patch("tap_airbyte.yarn.main.is_airbyte_app_running", side_effect=[True, True, False]) as mock_is_running, \
            patch("tap_airbyte.yarn.main.read_file", side_effect=[15, 30, 50]) as mock_read_file:
        stream_file(file_path, yarn_config, app_id)

        # Assert `is_airbyte_app_running` was called 3 times
        assert mock_is_running.call_count == 3
        mock_is_running.assert_any_call(yarn_config, app_id)

        # Assert `read_file` was called with the correct arguments
        mock_read_file.assert_any_call(file_path, 0)
        mock_read_file.assert_any_call(file_path, 15)
        mock_read_file.assert_any_call(file_path, 30)

        # Assert `sleep` was called the expected number of times
        assert mock_sleep.call_count == 3  # Two during the loop, one final sleep
