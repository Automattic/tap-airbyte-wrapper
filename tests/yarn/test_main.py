import json
from unittest.mock import MagicMock, patch

import pytest

from tap_airbyte.yarn.service import (
    CONTAINER_CONF_DIR, HELPER_PATH, WEBHDFS_MODULE_PATH, destroy_yarn_service, run_yarn_service,
)
from tap_airbyte.yarn.streaming import TimeoutException, read_file, stream_file, wait_for_file
from tap_airbyte.yarn.webhdfs import (
    WebHdfsClient, WebHdfsError, hdfs_delete, hdfs_file_length, hdfs_mkdirs, hdfs_read_file,
    hdfs_write_file,
)

YARN_CONFIG = {"base_url": "https://gateway.example.com", "username": "u", "password": "p"}


@pytest.fixture(autouse=True)
def mock_sleep():
    # Ensure the patch targets match the locations of `sleep` usage
    with patch("tap_airbyte.yarn.streaming.sleep", return_value=None) as mock, \
            patch("tap_airbyte.yarn.service.sleep", return_value=None):
        yield mock


def _response(status_code=200, json_data=None):
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = json_data
    if status_code >= 400:
        response.raise_for_status.side_effect = Exception(f"HTTP {status_code}")
    else:
        response.raise_for_status.return_value = None
    return response


# ---------------------------------------------------------------------------
# WebHDFS client (against the in-process fake gateway, see conftest.py)
# ---------------------------------------------------------------------------

def test_hdfs_write_file_follows_307_redirect_with_body(webhdfs):
    hdfs_write_file(webhdfs.yarn_config, "/tmp/f", "hello")

    assert webhdfs.files["/tmp/f"] == b"hello"
    assert webhdfs.permissions["/tmp/f"] == "600"
    # Body-less NameNode hop first (the fake 400s if a body shows up there),
    # then the body goes to the redirect target only.
    assert webhdfs.requests == [
        ("PUT", "/webhdfs/v1/tmp/f", "CREATE"),
        ("PUT", "/dn/webhdfs/v1/tmp/f", "CREATE"),
    ]


def test_hdfs_write_file_refuses_to_upload_without_redirect():
    """A gateway answering the NameNode hop with a success but no redirect
    would silently swallow the body — surface it instead."""
    client = WebHdfsClient("https://gw.example.com", "Basic x")
    with patch.object(WebHdfsClient, "_http", return_value=(201, {}, b"")) as mock_http:
        with pytest.raises(WebHdfsError, match="expected a redirect"):
            client.write_file("/tmp/f", b"payload")
    # Only the body-less NameNode hop was attempted.
    assert mock_http.call_count == 1
    assert mock_http.call_args.args[:2] == (
        "PUT", "https://gw.example.com/webhdfs/v1/tmp/f?op=CREATE&overwrite=true&permission=600")
    assert len(mock_http.call_args.args) == 2  # no data / from_file on the first hop


def test_hdfs_write_file_accepts_a_local_file_path(webhdfs, tmp_path):
    src = tmp_path / "chunk"
    src.write_bytes(b"from disk")
    client = WebHdfsClient.from_yarn_config(webhdfs.yarn_config)
    client.write_file("/tmp/f", from_file=str(src))
    assert webhdfs.files["/tmp/f"] == b"from disk"
    client.append_file("/tmp/f", from_file=str(src))
    assert webhdfs.files["/tmp/f"] == b"from diskfrom disk"


def test_hdfs_append_file(webhdfs):
    client = WebHdfsClient.from_yarn_config(webhdfs.yarn_config)
    client.write_file("/tmp/f", b"one\n")
    client.append_file("/tmp/f", b"two\n")
    assert webhdfs.files["/tmp/f"] == b"one\ntwo\n"
    assert webhdfs.requests[-2:] == [
        ("POST", "/webhdfs/v1/tmp/f", "APPEND"),
        ("POST", "/dn/webhdfs/v1/tmp/f", "APPEND"),
    ]


def test_hdfs_append_to_missing_file_raises(webhdfs):
    with pytest.raises(WebHdfsError) as exc:
        WebHdfsClient.from_yarn_config(webhdfs.yarn_config).append_file("/tmp/nope", b"x")
    assert exc.value.status == 404


def test_hdfs_write_file_without_overwrite_raises_when_exists(webhdfs):
    client = WebHdfsClient.from_yarn_config(webhdfs.yarn_config)
    client.write_file("/tmp/f", b"one")
    with pytest.raises(WebHdfsError) as exc:
        client.write_file("/tmp/f", b"two", overwrite=False)
    assert exc.value.status == 403
    assert webhdfs.files["/tmp/f"] == b"one"


def test_hdfs_file_length_returns_length(webhdfs):
    webhdfs.files["/tmp/f"] = b"x" * 42
    assert hdfs_file_length(webhdfs.yarn_config, "/tmp/f") == 42


def test_hdfs_file_length_returns_none_when_missing(webhdfs):
    assert hdfs_file_length(webhdfs.yarn_config, "/tmp/nope") is None


def test_hdfs_read_file_passes_offset(webhdfs):
    webhdfs.files["/tmp/f"] = b"line1\nline2\n"
    assert hdfs_read_file(webhdfs.yarn_config, "/tmp/f", offset=6) == b"line2\n"


def test_hdfs_read_file_returns_empty_when_missing(webhdfs):
    """A 404 between GETFILESTATUS and OPEN (file not created yet) means
    "no data yet", not an error."""
    assert hdfs_read_file(webhdfs.yarn_config, "/tmp/f", offset=6) == b""


def test_hdfs_mkdirs_defaults_to_owner_only(webhdfs):
    hdfs_mkdirs(webhdfs.yarn_config, "/user/u/.airbyte/run1")
    assert webhdfs.permissions["/user/u/.airbyte/run1"] == "700"


def test_hdfs_delete_recursive_and_best_effort(webhdfs):
    webhdfs.files.update({"/run/a": b"1", "/run/b": b"2", "/other": b"3"})
    hdfs_delete(webhdfs.yarn_config, "/run", recursive=True)
    assert webhdfs.files == {"/other": b"3"}
    # Unreachable endpoint: logged, not raised.
    hdfs_delete({**YARN_CONFIG, "webhdfs_base_url": "http://127.0.0.1:1"}, "/x")


def test_webhdfs_rejects_bad_credentials(webhdfs):
    config = {**webhdfs.yarn_config, "password": "wrong"}
    with pytest.raises(WebHdfsError) as exc:
        hdfs_file_length(config, "/tmp/f")
    assert exc.value.status == 401


def test_webhdfs_uses_webhdfs_base_url_override_and_extra_headers():
    config = {**YARN_CONFIG, "webhdfs_base_url": "https://webhdfs.example.com/",
              "extra_headers": {"X-Gateway": "1"}}
    client = WebHdfsClient.from_yarn_config(config)
    with patch.object(WebHdfsClient, "_http",
                      return_value=(200, {}, b'{"FileStatus": {"length": 1}}')) as mock_http:
        client.file_length("/tmp/f")
    method, url = mock_http.call_args.args[:2]
    assert (method, url) == ("GET", "https://webhdfs.example.com/webhdfs/v1/tmp/f?op=GETFILESTATUS")
    assert client.headers["X-Gateway"] == "1"
    assert client.headers["Authorization"] == "Basic dTpw"  # u:p


def test_webhdfs_credentials_roundtrip_carries_token_not_password(tmp_path):
    client = WebHdfsClient.from_yarn_config({**YARN_CONFIG, "extra_headers": {"X-A": "b"}})
    creds = json.loads(client.credentials_json())
    assert creds == {"base_url": "https://gateway.example.com", "authorization": "Basic dTpw",
                     "extra_headers": {"X-A": "b"}}
    assert "password" not in creds and "p" not in creds.values()
    path = tmp_path / "webhdfs.json"
    path.write_text(client.credentials_json())
    restored = WebHdfsClient.from_credentials_file(str(path))
    assert restored.headers == client.headers
    assert restored.base_url == client.base_url


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
    """File length equal to the current offset means no new data; don't
    attempt an out-of-range offset read."""
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
            patch("tap_airbyte.yarn.streaming.hdfs_delete") as mock_delete, \
            patch("tap_airbyte.yarn.streaming.destroy_yarn_service") as mock_destroy:
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

        # Clean run — the whole per-run HDFS scratch dir is removed and the
        # service (and its ~/.yarn/services dir) destroyed.
        mock_delete.assert_called_once_with(YARN_CONFIG, "/tmp/.airbyte/run1", recursive=True)
        mock_destroy.assert_called_once_with(YARN_CONFIG, app_id)


def test_stream_file_on_failure_deletes_credential_files_keeps_stdout():
    file_path = "/tmp/.airbyte/run1/stdout-read"
    with patch("tap_airbyte.yarn.streaming.is_airbyte_app_running",
               side_effect=Exception("Yarn application app_123 failed.")), \
            patch("tap_airbyte.yarn.streaming.hdfs_delete") as mock_delete, \
            patch("tap_airbyte.yarn.streaming.destroy_yarn_service") as mock_destroy:
        with pytest.raises(Exception, match="failed"):
            stream_file(file_path, YARN_CONFIG, "app_123")

    deleted = [call.args[1] for call in mock_delete.call_args_list]
    assert deleted == [
        "/tmp/.airbyte/run1/config.json",
        "/tmp/.airbyte/run1/state.json",
        "/tmp/.airbyte/run1/webhdfs.json",
    ]
    mock_destroy.assert_called_once_with(YARN_CONFIG, "app_123")


def test_destroy_yarn_service_deletes_service_by_app_name():
    session = MagicMock()
    session.delete.return_value = _response(204)
    with patch("tap_airbyte.yarn.service.get_yarn_service_application_info",
               return_value={"name": "source-slack-abc", "state": "FINISHED"}), \
            patch("tap_airbyte.yarn.service.create_session", return_value=session):
        destroy_yarn_service(YARN_CONFIG, "app_1")
    session.delete.assert_called_once_with("https://gateway.example.com/app/v1/services/source-slack-abc")


def test_destroy_yarn_service_is_best_effort(caplog):
    with patch("tap_airbyte.yarn.service.get_yarn_service_application_info",
               side_effect=Exception("RM down")):
        destroy_yarn_service(YARN_CONFIG, "app_1")  # must not raise
    assert "Failed to destroy YARN service" in caplog.text


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
            patch("tap_airbyte.yarn.service.hdfs_mkdirs") as mock_mkdirs, \
            patch("tap_airbyte.yarn.service.create_session", return_value=session), \
            patch("tap_airbyte.yarn.service._get_yarn_service_app_id", return_value="app_1"):
        app_id, hdfs_output_path = run_yarn_service(config, command, str(runtime_tmp_dir))

    assert app_id == "app_1"
    assert hdfs_output_path == "/tmp/.airbyte/tmpabc123/stdout-read"
    # Run dir created owner-only before any upload.
    mock_mkdirs.assert_called_once_with(YARN_CONFIG, "/tmp/.airbyte/tmpabc123")

    # Only the secret-bearing files are uploaded over WebHDFS (600) and
    # localized as STATIC; the rest is inlined in the spec as TEMPLATE
    # content, which the AM logs and keeps a copy of.
    uploads = {call.args[1]: call.args[2] for call in mock_write.call_args_list}
    assert set(uploads) == {
        "/tmp/.airbyte/tmpabc123/webhdfs.json",
        "/tmp/.airbyte/tmpabc123/config.json",
    }
    # Only the Basic token reaches the container, never username/password.
    creds = json.loads(uploads["/tmp/.airbyte/tmpabc123/webhdfs.json"])
    assert creds["authorization"] == "Basic dTpw"
    assert "password" not in creds

    service_config = session.post.call_args.kwargs["json"]
    component = service_config["components"][0]
    files = {f["dest_file"]: f for f in component["configuration"]["files"]}
    assert set(files) == {f"{CONTAINER_CONF_DIR}/{name}" for name in
                          ("helper.py", "webhdfs.py", "webhdfs.json", "launch.sh", "config.json", "catalog.json")}
    for name in ("webhdfs.json", "config.json"):
        assert files[f"{CONTAINER_CONF_DIR}/{name}"] == {
            "type": "STATIC", "dest_file": f"{CONTAINER_CONF_DIR}/{name}",
            "src_file": f"/tmp/.airbyte/tmpabc123/{name}"}
    for name in ("helper.py", "webhdfs.py", "launch.sh", "catalog.json"):
        entry = files[f"{CONTAINER_CONF_DIR}/{name}"]
        assert entry["type"] == "TEMPLATE" and "src_file" not in entry
        assert set(entry["properties"]) == {"content"}
    assert files[f"{CONTAINER_CONF_DIR}/catalog.json"]["properties"]["content"] == "{}"
    launch_script = files[f"{CONTAINER_CONF_DIR}/launch.sh"]["properties"]["content"]
    assert (f"python -u {CONTAINER_CONF_DIR}/helper.py /tmp/.airbyte/tmpabc123/stdout-read "
            f"{CONTAINER_CONF_DIR}/webhdfs.json") in launch_script
    assert f"python -u main.py {command} >/tmp/airbyte_pipe" in launch_script
    # Nothing secret in the spec (the AM logs it).
    assert "Basic dTpw" not in json.dumps(service_config)
    assert component["launch_command"] == f"{CONTAINER_CONF_DIR}/launch.sh"
    # No shared-mount plumbing left in the spec.
    assert "YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS" not in component["configuration"]["env"]


def test_run_yarn_service_never_logs_spec_or_secrets(tmp_path, monkeypatch, caplog):
    """The spec carries inline file content; secrets are in the uploads.
    Neither may show up in our logs, even at DEBUG."""
    import logging
    caplog.set_level(logging.DEBUG)
    monkeypatch.setenv("HDFS_PATH", "/tmp/.airbyte")
    runtime_tmp_dir = tmp_path / "tmpabc123"
    runtime_tmp_dir.mkdir()
    (runtime_tmp_dir / "config.json").write_text('{"api_token": "SUPERSECRET"}')
    (runtime_tmp_dir / "catalog.json").write_text('{"streams": ["CATALOGMARK"]}')
    config = {"yarn_service_config": YARN_CONFIG, "airbyte_spec": {"image": "airbyte/source-slack"}}
    session = MagicMock()
    session.post.return_value = _response(200, json_data={"uri": "v1/services/foo"})

    with patch("tap_airbyte.yarn.service.hdfs_write_file"), \
            patch("tap_airbyte.yarn.service.hdfs_mkdirs"), \
            patch("tap_airbyte.yarn.service.create_session", return_value=session), \
            patch("tap_airbyte.yarn.service._get_yarn_service_app_id", return_value="app_1"):
        run_yarn_service(config, "read", str(runtime_tmp_dir))

    assert "SUPERSECRET" not in caplog.text
    assert "CATALOGMARK" not in caplog.text
    assert "dTpw" not in caplog.text


def test_run_yarn_service_uploads_files_the_am_would_mangle(tmp_path, monkeypatch):
    """TEMPLATE content goes through the AM's ${TOKEN} / {{key}} substitution;
    a non-secret file containing such markers must be uploaded as STATIC instead."""
    monkeypatch.setenv("HDFS_PATH", "/tmp/.airbyte")
    runtime_tmp_dir = tmp_path / "tmpabc123"
    runtime_tmp_dir.mkdir()
    (runtime_tmp_dir / "catalog.json").write_text('{"name": "${USER}-stream"}')
    config = {"yarn_service_config": YARN_CONFIG, "airbyte_spec": {"image": "airbyte/source-slack"}}
    session = MagicMock()
    session.post.return_value = _response(200, json_data={"uri": "v1/services/foo"})

    with patch("tap_airbyte.yarn.service.hdfs_write_file") as mock_write, \
            patch("tap_airbyte.yarn.service.hdfs_mkdirs"), \
            patch("tap_airbyte.yarn.service.create_session", return_value=session), \
            patch("tap_airbyte.yarn.service._get_yarn_service_app_id", return_value="app_1"):
        run_yarn_service(config, "read", str(runtime_tmp_dir))

    uploaded = {call.args[1] for call in mock_write.call_args_list}
    assert "/tmp/.airbyte/tmpabc123/catalog.json" in uploaded
    files = {f["dest_file"]: f for f in session.post.call_args.kwargs["json"]["components"][0]["configuration"]["files"]}
    assert files[f"{CONTAINER_CONF_DIR}/catalog.json"]["type"] == "STATIC"


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
            patch("tap_airbyte.yarn.service.hdfs_mkdirs"), \
            patch("tap_airbyte.yarn.service.create_session", return_value=session), \
            patch("tap_airbyte.yarn.service._get_yarn_service_app_id", return_value="app_1"):
        _, hdfs_output_path = run_yarn_service(config, "read", str(runtime_tmp_dir))

    assert hdfs_output_path == "/user/u/.airbyte/tmpabc123/stdout-read"


def test_run_yarn_service_inlines_helper_modules_verbatim(tmp_path, monkeypatch):
    """helper.py / webhdfs.py inlined in the spec must be the modules shipped
    in the package, byte for byte, and free of template markers."""
    monkeypatch.setenv("HDFS_PATH", "/tmp/.airbyte")
    runtime_tmp_dir = tmp_path / "tmpabc123"
    runtime_tmp_dir.mkdir()
    config = {
        "yarn_service_config": YARN_CONFIG,
        "airbyte_spec": {"image": "airbyte/source-slack"},
    }
    session = MagicMock()
    session.post.return_value = _response(200, json_data={"uri": "v1/services/foo"})

    with patch("tap_airbyte.yarn.service.hdfs_write_file") as mock_write, \
            patch("tap_airbyte.yarn.service.hdfs_mkdirs"), \
            patch("tap_airbyte.yarn.service.create_session", return_value=session), \
            patch("tap_airbyte.yarn.service._get_yarn_service_app_id", return_value="app_1"):
        run_yarn_service(config, "read", str(runtime_tmp_dir))

    assert not mock_write.call_args_list or all(
        not call.args[1].endswith((".py", "launch.sh")) for call in mock_write.call_args_list)
    files = {f["dest_file"]: f for f in session.post.call_args.kwargs["json"]["components"][0]["configuration"]["files"]}
    for name, path in (("helper.py", HELPER_PATH), ("webhdfs.py", WEBHDFS_MODULE_PATH)):
        with open(path, "r", encoding="utf-8") as f:
            source = f.read()
        entry = files[f"{CONTAINER_CONF_DIR}/{name}"]
        assert entry["type"] == "TEMPLATE"
        assert entry["properties"]["content"] == source
        # Would be silently rewritten by the AM's substitution pass.
        assert "${" not in source and "{{" not in source
    # The in-container module must not depend on the package or third parties.
    webhdfs_src = files[f"{CONTAINER_CONF_DIR}/webhdfs.py"]["properties"]["content"]
    assert "import requests" not in webhdfs_src
    assert "from tap_airbyte" not in webhdfs_src

# ---------------------------------------------------------------------------
# yarn_config with declared-but-unset keys (meltano passes them as None)
# ---------------------------------------------------------------------------

def test_create_session_tolerates_none_extra_headers():
    from tap_airbyte.yarn.session import create_session
    session = create_session({**YARN_CONFIG, "extra_headers": None})
    assert session.headers["Content-Type"] == "application/json"
    assert session.auth.username == "u"


def test_run_yarn_service_tolerates_none_optional_keys(tmp_path, monkeypatch):
    monkeypatch.setenv("HDFS_PATH", "/tmp/.airbyte")
    runtime_tmp_dir = tmp_path / "tmpabc123"
    runtime_tmp_dir.mkdir()
    config = {
        "yarn_service_config": {**YARN_CONFIG, "extra_headers": None, "queue": None,
                                "timeout": None, "webhdfs_base_url": None},
        "airbyte_spec": {"image": "airbyte/source-slack"},
    }
    session = MagicMock()
    session.post.return_value = _response(200, json_data={"uri": "v1/services/foo"})
    with patch("tap_airbyte.yarn.service.hdfs_write_file") as mock_write, \
            patch("tap_airbyte.yarn.service.hdfs_mkdirs"), \
            patch("tap_airbyte.yarn.service.create_session", return_value=session), \
            patch("tap_airbyte.yarn.service._get_yarn_service_app_id", return_value="app_1"):
        run_yarn_service(config, "read", str(runtime_tmp_dir))
    assert session.post.call_args.kwargs["json"]["queue"] == "default"
    creds = json.loads(next(c.args[2] for c in mock_write.call_args_list if c.args[1].endswith("webhdfs.json")))
    assert creds["extra_headers"] == {}
    assert creds["base_url"] == "https://gateway.example.com"  # webhdfs_base_url None -> base_url


def test_run_yarn_service_uses_host_docker_network(tmp_path, monkeypatch):
    monkeypatch.setenv("HDFS_PATH", "/tmp/.airbyte")
    runtime_tmp_dir = tmp_path / "tmpabc123"
    runtime_tmp_dir.mkdir()
    config = {"yarn_service_config": YARN_CONFIG, "airbyte_spec": {"image": "airbyte/source-slack"}}
    session = MagicMock()
    session.post.return_value = _response(200, json_data={"uri": "v1/services/foo"})
    with patch("tap_airbyte.yarn.service.hdfs_write_file"), \
            patch("tap_airbyte.yarn.service.hdfs_mkdirs"), \
            patch("tap_airbyte.yarn.service.create_session", return_value=session), \
            patch("tap_airbyte.yarn.service._get_yarn_service_app_id", return_value="app_1"):
        run_yarn_service(config, "read", str(runtime_tmp_dir))
    conf = session.post.call_args.kwargs["json"]["components"][0]["configuration"]
    assert conf["properties"]["docker.network"] == "host"
    assert conf["env"] == {"YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE": "true"}
