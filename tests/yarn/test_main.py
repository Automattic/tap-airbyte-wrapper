import os
import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest

from tap_airbyte.yarn.service import _HDFS_PUT_HELPER, CONTAINER_CONF_DIR, run_yarn_service
from tap_airbyte.yarn.streaming import TimeoutException, read_file, stream_file, wait_for_file
from tap_airbyte.yarn.webhdfs import hdfs_file_length, hdfs_read_file, hdfs_write_file

YARN_CONFIG = {"base_url": "https://gateway.example.com", "username": "u", "password": "p"}


@pytest.fixture(autouse=True)
def mock_sleep():
    # Ensure the patch targets match the locations of `sleep` usage
    with patch("tap_airbyte.yarn.streaming.sleep", return_value=None) as mock, \
            patch("tap_airbyte.yarn.service.sleep", return_value=None):
        yield mock


def _response(status_code=200, json_data=None, content=b"", headers=None):
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = json_data
    response.content = content
    response.headers = headers or {}
    if status_code >= 400:
        response.raise_for_status.side_effect = Exception(f"HTTP {status_code}")
    else:
        response.raise_for_status.return_value = None
    return response


# ---------------------------------------------------------------------------
# WebHDFS client
# ---------------------------------------------------------------------------

def test_hdfs_write_file_follows_307_redirect_with_body():
    session = MagicMock()
    redirect = _response(307, headers={"Location": "https://gateway.example.com/dn/webhdfs/v1/tmp/f?op=CREATE"})
    created = _response(201)
    session.request.side_effect = [redirect, created]
    with patch("tap_airbyte.yarn.webhdfs.create_session", return_value=session):
        hdfs_write_file(YARN_CONFIG, "/tmp/f", "hello")

    first, second = session.request.call_args_list
    assert first.args == ("PUT", "https://gateway.example.com/webhdfs/v1/tmp/f")
    assert first.kwargs["params"] == {"op": "CREATE", "overwrite": "true"}
    assert first.kwargs["data"] == b"hello"
    assert first.kwargs["allow_redirects"] is False
    # Redirect target hit with the same method and body.
    assert second.args == ("PUT", redirect.headers["Location"])
    assert second.kwargs["data"] == b"hello"


def test_hdfs_write_file_direct_response_without_redirect():
    session = MagicMock()
    session.request.return_value = _response(201)
    with patch("tap_airbyte.yarn.webhdfs.create_session", return_value=session):
        hdfs_write_file(YARN_CONFIG, "/tmp/f", b"payload")
    assert session.request.call_count == 1


def test_hdfs_file_length_returns_length():
    session = MagicMock()
    session.request.return_value = _response(200, json_data={"FileStatus": {"length": 42}})
    with patch("tap_airbyte.yarn.webhdfs.create_session", return_value=session):
        assert hdfs_file_length(YARN_CONFIG, "/tmp/f") == 42
    assert session.request.call_args.kwargs["params"] == {"op": "GETFILESTATUS"}


def test_hdfs_file_length_returns_none_when_missing():
    session = MagicMock()
    session.request.return_value = _response(404)
    with patch("tap_airbyte.yarn.webhdfs.create_session", return_value=session):
        assert hdfs_file_length(YARN_CONFIG, "/tmp/nope") is None


def test_hdfs_read_file_passes_offset():
    session = MagicMock()
    session.request.return_value = _response(200, content=b"line2\n")
    with patch("tap_airbyte.yarn.webhdfs.create_session", return_value=session):
        assert hdfs_read_file(YARN_CONFIG, "/tmp/f", offset=6) == b"line2\n"
    assert session.request.call_args.kwargs["params"] == {"op": "OPEN", "offset": "6"}


def test_hdfs_read_file_returns_empty_when_missing():
    """`hdfs dfs -put -f` deletes + recreates the destination — a 404 mid-commit
    means "no data yet", not an error."""
    session = MagicMock()
    session.request.return_value = _response(404)
    with patch("tap_airbyte.yarn.webhdfs.create_session", return_value=session):
        assert hdfs_read_file(YARN_CONFIG, "/tmp/f", offset=6) == b""


def test_webhdfs_uses_webhdfs_base_url_override():
    session = MagicMock()
    session.request.return_value = _response(200, json_data={"FileStatus": {"length": 1}})
    config = {**YARN_CONFIG, "webhdfs_base_url": "https://webhdfs.example.com/"}
    with patch("tap_airbyte.yarn.webhdfs.create_session", return_value=session):
        hdfs_file_length(config, "/tmp/f")
    assert session.request.call_args.args[1] == "https://webhdfs.example.com/webhdfs/v1/tmp/f"


# ---------------------------------------------------------------------------
# read_file
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "start_position, expected_output, expected_position",
    [
        (0, b"line1\nline2\nline3\n", len(b"line1\nline2\nline3\n")),  # Start from the beginning
        (6, b"line2\nline3\n", len(b"line1\nline2\nline3\n")),         # Start from the middle
    ]
)
def test_read_file(capfdbinary, start_position, expected_output, expected_position):
    file_content = b"line1\nline2\nline3\n"
    with patch("tap_airbyte.yarn.streaming.hdfs_file_length", return_value=len(file_content)), \
            patch("tap_airbyte.yarn.streaming.hdfs_read_file",
                  side_effect=lambda cfg, path, offset: file_content[offset:]) as mock_read:
        position = read_file(YARN_CONFIG, "/tmp/f", start_position)

    assert position == expected_position
    mock_read.assert_called_once_with(YARN_CONFIG, "/tmp/f", offset=start_position)
    assert capfdbinary.readouterr().out == expected_output


def test_read_file_returns_position_when_file_missing(capfdbinary):
    with patch("tap_airbyte.yarn.streaming.hdfs_file_length", return_value=None), \
            patch("tap_airbyte.yarn.streaming.hdfs_read_file") as mock_read:
        assert read_file(YARN_CONFIG, "/tmp/f", 10) == 10
    mock_read.assert_not_called()
    assert capfdbinary.readouterr().out == b""


def test_read_file_returns_position_when_no_new_bytes(capfdbinary):
    """File shorter than (or equal to) the current offset — e.g. mid-commit —
    means no new data; don't attempt an out-of-range offset read."""
    with patch("tap_airbyte.yarn.streaming.hdfs_file_length", return_value=10), \
            patch("tap_airbyte.yarn.streaming.hdfs_read_file") as mock_read:
        assert read_file(YARN_CONFIG, "/tmp/f", 10) == 10
    mock_read.assert_not_called()


def test_read_file_holds_back_incomplete_final_line(capfdbinary):
    content = b"done\npartial line without newline"
    with patch("tap_airbyte.yarn.streaming.hdfs_file_length", return_value=len(content)), \
            patch("tap_airbyte.yarn.streaming.hdfs_read_file", return_value=content):
        assert read_file(YARN_CONFIG, "/tmp/f", 0) == len(b"done\n")
    assert capfdbinary.readouterr().out == b"done\n"


# ---------------------------------------------------------------------------
# wait_for_file
# ---------------------------------------------------------------------------

YARN_RUNNING = {"state": "RUNNING", "finalStatus": "UNDEFINED"}
YARN_FINISHED = {"state": "FINISHED", "finalStatus": "SUCCEEDED"}
YARN_FAILED = {"state": "FAILED", "finalStatus": "FAILED"}


def test_wait_for_file_success_file_appears_mid_poll():
    with patch("tap_airbyte.yarn.streaming.hdfs_file_length",
               side_effect=[None, None, 10]) as mock_length, \
            patch("tap_airbyte.yarn.streaming.get_yarn_service_application_info",
                  return_value=YARN_RUNNING):
        wait_for_file("/tmp/testfile", YARN_CONFIG, "app_1", timeout=10, interval=1)
        assert mock_length.call_count == 3


def test_wait_for_file_returns_immediately_when_file_already_present():
    with patch("tap_airbyte.yarn.streaming.hdfs_file_length", return_value=42), \
            patch("tap_airbyte.yarn.streaming.get_yarn_service_application_info") as mock_info:
        wait_for_file("/tmp/testfile", YARN_CONFIG, "app_1", timeout=10, interval=1)
        # No need to poll YARN if the file is already there.
        mock_info.assert_not_called()


def test_wait_for_file_ignores_zero_byte_file():
    """An empty file shouldn't satisfy the wait — keep polling."""
    with patch("tap_airbyte.yarn.streaming.hdfs_file_length", return_value=0), \
            patch("tap_airbyte.yarn.streaming.get_yarn_service_application_info",
                  return_value=YARN_RUNNING):
        with pytest.raises(TimeoutException):
            wait_for_file("/tmp/testfile", YARN_CONFIG, "app_1", timeout=2, interval=1)


def test_wait_for_file_raises_on_yarn_failure():
    """If YARN reports FAILED, bail out immediately rather than waiting out the timeout."""
    with patch("tap_airbyte.yarn.streaming.hdfs_file_length", return_value=None), \
            patch("tap_airbyte.yarn.streaming.get_yarn_service_application_info",
                  return_value=YARN_FAILED):
        with pytest.raises(Exception, match="Yarn application app_1 failed"):
            wait_for_file("/tmp/testfile", YARN_CONFIG, "app_1", timeout=60, interval=1)


def test_wait_for_file_keeps_polling_after_yarn_finishes_cleanly():
    """A successful YARN run is *not* a failure — keep polling so the helper's
    final commit can still satisfy the wait (or eventually time out)."""
    with patch("tap_airbyte.yarn.streaming.hdfs_file_length", return_value=None), \
            patch("tap_airbyte.yarn.streaming.get_yarn_service_application_info",
                  return_value=YARN_FINISHED):
        with pytest.raises(TimeoutException, match="File not created after 3"):
            wait_for_file("/tmp/testfile", YARN_CONFIG, "app_1", timeout=3, interval=1)


def test_wait_for_file_returns_when_file_appears_after_yarn_finishes():
    """If YARN finishes cleanly and the final commit lands afterwards,
    wait_for_file should still succeed."""
    with patch("tap_airbyte.yarn.streaming.hdfs_file_length",
               side_effect=[None, None, 10]) as mock_length, \
            patch("tap_airbyte.yarn.streaming.get_yarn_service_application_info",
                  return_value=YARN_FINISHED):
        wait_for_file("/tmp/testfile", YARN_CONFIG, "app_1", timeout=10, interval=1)
        assert mock_length.call_count == 3


def test_wait_for_file_timeout_when_file_never_appears():
    with patch("tap_airbyte.yarn.streaming.hdfs_file_length", return_value=None), \
            patch("tap_airbyte.yarn.streaming.get_yarn_service_application_info",
                  return_value=YARN_RUNNING):
        with pytest.raises(TimeoutException,
                           match="File not created after 5: /tmp/nope"):
            wait_for_file("/tmp/nope", YARN_CONFIG, "app_1", timeout=5, interval=1)


# ---------------------------------------------------------------------------
# stream_file
# ---------------------------------------------------------------------------

def test_stream_file(mock_sleep):
    file_path = "/tmp/.airbyte/run1/stdout-read"
    app_id = "app_123"

    # Mock `is_airbyte_app_running` to return True twice, then False
    with patch("tap_airbyte.yarn.streaming.is_airbyte_app_running", side_effect=[True, True, False]) as mock_is_running, \
            patch("tap_airbyte.yarn.streaming.read_file", side_effect=[15, 30, 50]) as mock_read_file, \
            patch("tap_airbyte.yarn.streaming.hdfs_delete") as mock_delete:
        stream_file(file_path, YARN_CONFIG, app_id)

        # Assert `is_airbyte_app_running` was called 3 times
        assert mock_is_running.call_count == 3
        mock_is_running.assert_any_call(YARN_CONFIG, app_id)

        # Assert `read_file` was called with the correct arguments
        mock_read_file.assert_any_call(YARN_CONFIG, file_path, 0)
        mock_read_file.assert_any_call(YARN_CONFIG, file_path, 15)
        mock_read_file.assert_any_call(YARN_CONFIG, file_path, 30)

        # Assert `sleep` was called the expected number of times
        assert mock_sleep.call_count == 3  # Two during the loop, one final sleep

        # Clean run — the whole per-run HDFS scratch dir is removed.
        mock_delete.assert_called_once_with(YARN_CONFIG, "/tmp/.airbyte/run1", recursive=True)


def test_stream_file_on_failure_deletes_credential_files_keeps_stdout():
    file_path = "/tmp/.airbyte/run1/stdout-read"
    with patch("tap_airbyte.yarn.streaming.is_airbyte_app_running",
               side_effect=Exception("Yarn application app_123 failed.")), \
            patch("tap_airbyte.yarn.streaming.hdfs_delete") as mock_delete:
        with pytest.raises(Exception, match="failed"):
            stream_file(file_path, YARN_CONFIG, "app_123")

    deleted = [call.args[1] for call in mock_delete.call_args_list]
    assert deleted == [
        "/tmp/.airbyte/run1/config.json",
        "/tmp/.airbyte/run1/catalog.json",
        "/tmp/.airbyte/run1/state.json",
    ]


# ---------------------------------------------------------------------------
# run_yarn_service
# ---------------------------------------------------------------------------

def test_run_yarn_service_uploads_files_and_localizes_them(tmp_path, monkeypatch):
    monkeypatch.setenv("HDFS_PATH", "/tmp/.airbyte")
    runtime_tmp_dir = tmp_path / "tmpabc123"
    runtime_tmp_dir.mkdir()
    (runtime_tmp_dir / "config.json").write_bytes(b"{}")
    (runtime_tmp_dir / "catalog.json").write_bytes(b"{}")

    config = {
        "yarn_service_config": YARN_CONFIG,
        "airbyte_spec": {"image": "airbyte/source-slack", "tag": "1.0"},
    }
    session = MagicMock()
    session.post.return_value = _response(200, json_data={"uri": "v1/services/foo"})
    command = f"read --config {CONTAINER_CONF_DIR}/config.json --catalog {CONTAINER_CONF_DIR}/catalog.json"

    with patch("tap_airbyte.yarn.service.hdfs_write_file") as mock_write, \
            patch("tap_airbyte.yarn.service.create_session", return_value=session), \
            patch("tap_airbyte.yarn.service._get_yarn_service_app_id", return_value="app_1"):
        app_id, hdfs_output_path = run_yarn_service(config, command, str(runtime_tmp_dir))

    assert app_id == "app_1"
    assert hdfs_output_path == "/tmp/.airbyte/tmpabc123/stdout-read"

    # helper.py, launch.sh, and both staged files are uploaded over WebHDFS.
    uploaded = {call.args[1] for call in mock_write.call_args_list}
    assert uploaded == {
        "/tmp/.airbyte/tmpabc123/helper.py",
        "/tmp/.airbyte/tmpabc123/launch.sh",
        "/tmp/.airbyte/tmpabc123/config.json",
        "/tmp/.airbyte/tmpabc123/catalog.json",
    }
    launch_script = next(
        call.args[2] for call in mock_write.call_args_list
        if call.args[1].endswith("launch.sh")
    )
    assert f"python -u {CONTAINER_CONF_DIR}/helper.py /tmp/.airbyte/tmpabc123/stdout-read" in launch_script
    assert f"python -u main.py {command} >/tmp/airbyte_pipe" in launch_script

    service_config = session.post.call_args.kwargs["json"]
    component = service_config["components"][0]
    # Every uploaded file is localized into the container conf dir.
    assert {
        (f["src_file"], f["dest_file"], f["type"]) for f in component["files"]
    } == {
        (f"/tmp/.airbyte/tmpabc123/{name}", f"{CONTAINER_CONF_DIR}/{name}", "STATIC")
        for name in ("helper.py", "launch.sh", "config.json", "catalog.json")
    }
    assert component["launch_command"] == f"{CONTAINER_CONF_DIR}/launch.sh"
    # No shared-mount plumbing left in the spec.
    assert "YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS" not in component["configuration"]["env"]


def test_run_yarn_service_defaults_hdfs_base_to_user_home(tmp_path, monkeypatch):
    """Without HDFS_PATH, stage under the submitting user's HDFS home
    rather than a global /tmp path."""
    monkeypatch.delenv("HDFS_PATH", raising=False)
    runtime_tmp_dir = tmp_path / "tmpabc123"
    runtime_tmp_dir.mkdir()

    config = {
        "yarn_service_config": YARN_CONFIG,
        "airbyte_spec": {"image": "airbyte/source-slack"},
    }
    session = MagicMock()
    session.post.return_value = _response(200, json_data={"uri": "v1/services/foo"})

    with patch("tap_airbyte.yarn.service.hdfs_write_file"), \
            patch("tap_airbyte.yarn.service.create_session", return_value=session), \
            patch("tap_airbyte.yarn.service._get_yarn_service_app_id", return_value="app_1"):
        _, hdfs_output_path = run_yarn_service(config, "read", str(runtime_tmp_dir))

    assert hdfs_output_path == "/user/u/.airbyte/tmpabc123/stdout-read"


# ---------------------------------------------------------------------------
# _HDFS_PUT_HELPER (the snippet executed inside the Airbyte container)
#
# The helper invokes `hdfs dfs -put -f local_buf hdfs_path` per commit,
# which we don't have in the test environment. Stub `hdfs` to a fake script
# that copies the local buf to the destination path, so tests exercise the
# real buffering / loop / commit logic without needing Hadoop installed.
# ---------------------------------------------------------------------------

def _run_helper(tmp_path, stdin_bytes):
    output_path = tmp_path / "output.log"
    local_buf = tmp_path / "airbyte_buf"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    # Fake `hdfs` shim — only handles `hdfs dfs -put -f <src> <dst>`.
    fake_hdfs = bin_dir / "hdfs"
    fake_hdfs.write_text(
        "#!/bin/sh\n"
        '[ "$1" = "dfs" ] && [ "$2" = "-put" ] && [ "$3" = "-f" ] || exit 99\n'
        'cp -f "$4" "$5"\n'
    )
    fake_hdfs.chmod(0o755)
    helper_code = _HDFS_PUT_HELPER.replace("/tmp/airbyte_buf", str(local_buf))
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ.get('PATH', '')}"}
    proc = subprocess.run(
        [sys.executable, "-c", helper_code, str(output_path)],
        input=stdin_bytes,
        capture_output=True,
        timeout=10,
        env=env,
    )
    return proc, output_path, local_buf


def test_hdfs_put_helper_writes_all_input_to_output(tmp_path):
    stdin = b"line1\nline2\nline3\n"
    proc, output_path, _ = _run_helper(tmp_path, stdin)

    assert proc.returncode == 0, proc.stderr.decode()
    assert output_path.read_bytes() == stdin


def test_hdfs_put_helper_handles_empty_input(tmp_path):
    """Connectors that produce no records should still finalize an empty file."""
    proc, output_path, _ = _run_helper(tmp_path, b"")

    assert proc.returncode == 0, proc.stderr.decode()
    assert output_path.exists()
    assert output_path.read_bytes() == b""


def test_hdfs_put_helper_preserves_partial_final_line(tmp_path):
    """Final line with no trailing newline must still be written."""
    stdin = b"line1\nline2 (no newline)"
    proc, output_path, _ = _run_helper(tmp_path, stdin)

    assert proc.returncode == 0, proc.stderr.decode()
    assert output_path.read_bytes() == stdin


def test_hdfs_put_helper_local_buf_matches_output(tmp_path):
    """The local-disk buffer is left behind on disk; verify it contains the
    same bytes we copied to the output (sanity check on the buffering path)."""
    stdin = b"alpha\nbeta\ngamma\n"
    proc, output_path, local_buf = _run_helper(tmp_path, stdin)

    assert proc.returncode == 0
    # Local buffer holds the same bytes as the committed output.
    assert local_buf.read_bytes() == stdin
    assert output_path.read_bytes() == stdin


def test_hdfs_put_helper_invokes_hdfs_put(tmp_path):
    """Sanity check that the helper actually shells out to `hdfs dfs -put -f`
    (not e.g. shutil.copyfile). Use a shim that records its argv."""
    output_path = tmp_path / "output.log"
    local_buf = tmp_path / "airbyte_buf"
    record_file = tmp_path / "hdfs_calls.txt"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake_hdfs = bin_dir / "hdfs"
    fake_hdfs.write_text(
        "#!/bin/sh\n"
        f'echo "$@" >> "{record_file}"\n'
        'cp -f "$4" "$5"\n'
    )
    fake_hdfs.chmod(0o755)
    helper_code = _HDFS_PUT_HELPER.replace("/tmp/airbyte_buf", str(local_buf))
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ.get('PATH', '')}"}
    proc = subprocess.run(
        [sys.executable, "-c", helper_code, str(output_path)],
        input=b"hello\n",
        capture_output=True,
        timeout=10,
        env=env,
    )
    assert proc.returncode == 0, proc.stderr.decode()
    calls = record_file.read_text().splitlines()
    assert calls, "hdfs was never invoked"
    # Every recorded call should be a `dfs -put -f <local> <hdfs>`.
    for call in calls:
        parts = call.split()
        assert parts[:3] == ["dfs", "-put", "-f"]
        assert parts[3] == str(local_buf)
        assert parts[4] == str(output_path)