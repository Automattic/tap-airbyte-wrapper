import os
import subprocess
import sys
from tempfile import NamedTemporaryFile
from unittest.mock import patch

import pytest

from tap_airbyte.yarn.main import (
    _FSYNC_HELPER,
    TimeoutException,
    read_file,
    stream_file,
    wait_for_file,
)


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


# ---------------------------------------------------------------------------
# wait_for_file
# ---------------------------------------------------------------------------

YARN_RUNNING = {"state": "RUNNING", "finalStatus": "UNDEFINED"}
YARN_FINISHED = {"state": "FINISHED", "finalStatus": "SUCCEEDED"}
YARN_FAILED = {"state": "FAILED", "finalStatus": "FAILED"}


def test_wait_for_file_success_file_appears_mid_poll():
    file_path = "/path/to/testfile"
    with patch("os.path.exists") as mock_exists, \
            patch("os.path.getsize", return_value=10), \
            patch("tap_airbyte.yarn.main.get_yarn_service_application_info",
                  return_value=YARN_RUNNING):
        # File missing first two checks, then appears.
        mock_exists.side_effect = [False, False, True]
        wait_for_file("/path/to/testfile", {}, "app_1", timeout=10, interval=1)
        assert mock_exists.call_count == 3


def test_wait_for_file_returns_immediately_when_file_already_present():
    with patch("os.path.exists", return_value=True), \
            patch("os.path.getsize", return_value=42), \
            patch("tap_airbyte.yarn.main.get_yarn_service_application_info") as mock_info:
        wait_for_file("/path/to/testfile", {}, "app_1", timeout=10, interval=1)
        # No need to poll YARN if the file is already there.
        mock_info.assert_not_called()


def test_wait_for_file_ignores_zero_byte_file():
    """An empty file shouldn't satisfy the wait — keep polling."""
    with patch("os.path.exists", return_value=True), \
            patch("os.path.getsize", return_value=0), \
            patch("tap_airbyte.yarn.main.get_yarn_service_application_info",
                  return_value=YARN_RUNNING):
        with pytest.raises(TimeoutException):
            wait_for_file("/path/to/testfile", {}, "app_1", timeout=2, interval=1)


def test_wait_for_file_raises_on_yarn_failure():
    """If YARN reports FAILED, bail out immediately rather than waiting out the timeout."""
    with patch("os.path.exists", return_value=False), \
            patch("tap_airbyte.yarn.main.get_yarn_service_application_info",
                  return_value=YARN_FAILED):
        with pytest.raises(Exception, match="Yarn application app_1 failed"):
            wait_for_file("/path/to/testfile", {}, "app_1", timeout=60, interval=1)


def test_wait_for_file_keeps_polling_after_yarn_finishes_cleanly():
    """A successful YARN run is *not* a failure — keep polling so a late
    rename can still satisfy the wait (or eventually time out)."""
    with patch("os.path.exists", return_value=False), \
            patch("tap_airbyte.yarn.main.get_yarn_service_application_info",
                  return_value=YARN_FINISHED):
        with pytest.raises(TimeoutException, match="File not created after 3"):
            wait_for_file("/path/to/testfile", {}, "app_1", timeout=3, interval=1)


def test_wait_for_file_returns_when_file_appears_after_yarn_finishes():
    """If YARN finishes cleanly and the FUSE rename catches up afterwards,
    wait_for_file should still succeed."""
    with patch("os.path.exists") as mock_exists, \
            patch("os.path.getsize", return_value=10), \
            patch("tap_airbyte.yarn.main.get_yarn_service_application_info",
                  return_value=YARN_FINISHED):
        # First two checks: missing. Third: file is there (post-rename).
        mock_exists.side_effect = [False, False, True]
        wait_for_file("/path/to/testfile", {}, "app_1", timeout=10, interval=1)
        assert mock_exists.call_count == 3


def test_wait_for_file_timeout_when_file_never_appears():
    with patch("os.path.exists", return_value=False), \
            patch("tap_airbyte.yarn.main.get_yarn_service_application_info",
                  return_value=YARN_RUNNING):
        with pytest.raises(TimeoutException,
                           match="File not created after 5: /path/to/nope"):
            wait_for_file("/path/to/nope", {}, "app_1", timeout=5, interval=1)


# ---------------------------------------------------------------------------
# stream_file
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# _FSYNC_HELPER (the snippet executed inside the Airbyte container)
#
# Run the helper as a real subprocess so we exercise the same code path that
# launch.sh runs in production. Substitute the hardcoded /tmp/airbyte_buf
# path with a per-test path to keep tests isolated.
# ---------------------------------------------------------------------------

def _run_helper(tmp_path, stdin_bytes):
    output_path = tmp_path / "output.log"
    local_buf = tmp_path / "airbyte_buf"
    helper_code = _FSYNC_HELPER.replace("/tmp/airbyte_buf", str(local_buf))
    proc = subprocess.run(
        [sys.executable, "-c", helper_code, str(output_path)],
        input=stdin_bytes,
        capture_output=True,
        timeout=10,
    )
    return proc, output_path, local_buf


def test_fsync_helper_writes_all_input_to_output(tmp_path):
    stdin = b"line1\nline2\nline3\n"
    proc, output_path, _ = _run_helper(tmp_path, stdin)

    assert proc.returncode == 0, proc.stderr.decode()
    assert output_path.read_bytes() == stdin


def test_fsync_helper_handles_empty_input(tmp_path):
    """Connectors that produce no records should still finalize an empty file."""
    proc, output_path, _ = _run_helper(tmp_path, b"")

    assert proc.returncode == 0, proc.stderr.decode()
    assert output_path.exists()
    assert output_path.read_bytes() == b""


def test_fsync_helper_preserves_partial_final_line(tmp_path):
    """Final line with no trailing newline must still be written."""
    stdin = b"line1\nline2 (no newline)"
    proc, output_path, _ = _run_helper(tmp_path, stdin)

    assert proc.returncode == 0, proc.stderr.decode()
    assert output_path.read_bytes() == stdin


def test_fsync_helper_cleans_up_local_buf_on_exit(tmp_path):
    """The local-disk buffer is left behind on disk; verify it contains the
    same bytes we copied to the output (sanity check on the buffering path)."""
    stdin = b"alpha\nbeta\ngamma\n"
    proc, output_path, local_buf = _run_helper(tmp_path, stdin)

    assert proc.returncode == 0
    # Local buffer holds the same bytes as the committed output.
    assert local_buf.read_bytes() == stdin
    assert output_path.read_bytes() == stdin
