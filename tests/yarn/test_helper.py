"""
Tests for tap_airbyte/yarn/helper.py — the stdout relay executed inside the
Airbyte container.

Unit tests drive HdfsAppender against an in-memory fake client. End-to-end
tests run the module as a real subprocess (the way the container does, from
a bare directory holding helper.py + webhdfs.py + webhdfs.json) against the
fake WebHDFS gateway from conftest.py, so threading / timer / signal /
redirect paths are all exercised without Hadoop.
"""
import io
import os
import shutil
import signal
import subprocess
import sys
import time

import pytest

from tap_airbyte.yarn import helper
from tap_airbyte.yarn.helper import HdfsAppender
from tap_airbyte.yarn.webhdfs import WebHdfsClient, WebHdfsError

HELPER_PATH = helper.__file__
WEBHDFS_PATH = os.path.join(os.path.dirname(HELPER_PATH), "webhdfs.py")


# ---------------------------------------------------------------------------
# HdfsAppender unit tests (in-memory fake client)
# ---------------------------------------------------------------------------

class FakeClient:
    """Records every write and simulates the remote file. `fail_writes` is a
    list of `landed` byte counts: each entry fails one write after keeping
    that prefix of the body (HDFS may persist part of a failed append)."""

    def __init__(self, fail_writes=()):
        self.remote = None
        self.writes = []  # (op, bytes)
        self.fail_writes = list(fail_writes)

    def _body(self, from_file):
        with open(from_file, "rb") as f:
            return f.read()

    def _maybe_fail(self, op, body, base):
        if self.fail_writes:
            landed = self.fail_writes.pop(0)
            self.remote = base + body[:landed]
            raise WebHdfsError(op, "/x", 500, b"IOException")

    def write_file(self, path, data=b"", permission="600", overwrite=True, from_file=None):
        body = self._body(from_file)
        if self.remote is not None and not overwrite:
            raise WebHdfsError("PUT", path, 403, b"FileAlreadyExistsException")
        self._maybe_fail("CREATE", body, b"")
        self.remote = body
        self.writes.append(("CREATE", body))

    def append_file(self, path, data=b"", from_file=None):
        body = self._body(from_file)
        if self.remote is None:
            raise WebHdfsError("POST", path, 404, b"FileNotFoundException")
        self._maybe_fail("APPEND", body, self.remote)
        self.remote += body
        self.writes.append(("APPEND", body))

    def file_length(self, path):
        return None if self.remote is None else len(self.remote)


def _appender(tmp_path, client, backoff=(0, 0, 0)):
    return HdfsAppender(client, "/hdfs/stdout", str(tmp_path / "buf"), str(tmp_path / "chunk"),
                        backoff=backoff, sleep=lambda _s: None)


def test_commit_creates_then_appends_only_new_bytes(tmp_path):
    client = FakeClient()
    a = _appender(tmp_path, client)
    a.write(b"line1\n")
    a.commit()
    a.write(b"line2\n")
    a.write(b"line3\n")
    a.commit()
    a.close()

    assert client.writes == [("CREATE", b"line1\n"), ("APPEND", b"line2\nline3\n")]
    assert client.remote == b"line1\nline2\nline3\n"
    assert a.committed == len(client.remote)


def test_commit_skips_round_trip_when_nothing_new(tmp_path):
    client = FakeClient()
    a = _appender(tmp_path, client)
    a.write(b"x\n")
    a.commit()
    a.commit()
    a.commit(final=True)
    a.close()
    assert len(client.writes) == 1


def test_final_commit_with_empty_input_creates_file(tmp_path):
    """Connectors that produce no records should still finalize an empty file."""
    client = FakeClient()
    a = _appender(tmp_path, client)
    a.commit()  # non-final, nothing buffered: no round-trip
    assert client.writes == []
    a.commit(final=True)
    a.close()
    assert client.writes == [("CREATE", b"")]
    assert client.remote == b""


def test_commit_retries_after_failure(tmp_path):
    client = FakeClient(fail_writes=[0])
    a = _appender(tmp_path, client)
    a.write(b"abc\n")
    a.commit()
    a.close()
    assert client.remote == b"abc\n"
    assert a.committed == 4


def test_commit_retries_after_transport_error(tmp_path):
    class Flaky(FakeClient):
        def __init__(self):
            super().__init__()
            self.calls = 0

        def write_file(self, *args, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise OSError("connection reset")  # URLError / timeout family
            super().write_file(*args, **kwargs)

    client = Flaky()
    a = _appender(tmp_path, client)
    a.write(b"abc\n")
    a.commit()
    a.close()
    assert client.remote == b"abc\n"


def test_retry_resumes_after_partially_landed_chunk(tmp_path):
    """If the failed write already stored a prefix of the chunk, the retry
    must skip those bytes rather than duplicate them — and switch from
    CREATE to APPEND since the file now exists."""
    client = FakeClient(fail_writes=[4])
    a = _appender(tmp_path, client)
    a.write(b"one\ntwo\n")
    a.commit()
    a.close()
    assert client.remote == b"one\ntwo\n"
    assert client.writes == [("APPEND", b"two\n")]
    assert a.committed == 8


def test_retry_skips_write_when_whole_chunk_landed(tmp_path):
    """Failure reported after HDFS actually took everything: no re-send."""
    client = FakeClient(fail_writes=[8])
    a = _appender(tmp_path, client)
    a.write(b"one\ntwo\n")
    a.commit()
    a.close()
    assert client.remote == b"one\ntwo\n"
    assert client.writes == []
    assert a.committed == 8


def test_commit_raises_after_retries_exhausted(tmp_path):
    client = FakeClient(fail_writes=[0, 0, 0])
    a = _appender(tmp_path, client, backoff=(0, 0))
    a.write(b"x\n")
    with pytest.raises(WebHdfsError):
        a.commit()
    a.close()
    assert a.committed == 0


def test_remote_length_zero_when_missing_or_unreachable(tmp_path):
    class Down(FakeClient):
        def file_length(self, path):
            raise OSError("down")

    a = _appender(tmp_path, FakeClient())
    assert a.remote_length() == 0
    a.close()
    a = _appender(tmp_path, Down())
    assert a.remote_length() == 0
    a.close()


def test_run_raises_when_gateway_unreachable(tmp_path, capsys):
    """Gateway down for the whole retry window → fail loudly (after retries),
    not swallow stdout silently."""
    client = WebHdfsClient("http://127.0.0.1:1", "Basic x", timeout=1)
    a = HdfsAppender(client, "/hdfs/stdout", str(tmp_path / "buf"), str(tmp_path / "chunk"),
                     backoff=(0,), sleep=lambda _s: None)
    with pytest.raises(OSError):
        helper.run(io.BytesIO(b"x\n"), a, interval=0.05)
    assert "HDFS commit failed" in capsys.readouterr().err
    assert a.buf.closed
    assert a.committed == 0


# ---------------------------------------------------------------------------
# End-to-end: helper.py as a subprocess against the fake WebHDFS gateway
# ---------------------------------------------------------------------------

HDFS_OUT = "/user/u/.airbyte/run1/stdout-read"


@pytest.fixture
def container(tmp_path, webhdfs):
    """Mimic the localized container dir: bare copies of the two modules plus
    the credentials file, and a clean environment (no tap_airbyte on path)."""
    conf = tmp_path / "airbyte"
    conf.mkdir()
    shutil.copy(HELPER_PATH, conf / "helper.py")
    shutil.copy(WEBHDFS_PATH, conf / "webhdfs.py")
    creds = conf / "webhdfs.json"
    creds.write_text(WebHdfsClient.from_yarn_config(webhdfs.yarn_config).credentials_json())
    buf_dir = tmp_path / "bufdir"
    buf_dir.mkdir()
    env = {k: v for k, v in os.environ.items() if k not in ("PYTHONPATH", "VIRTUAL_ENV")}
    env["AIRBYTE_HELPER_BUF_DIR"] = str(buf_dir)
    return {"conf": conf, "creds": creds, "buf": buf_dir / "airbyte_buf", "env": env, "server": webhdfs}


def _argv(container, interval):
    return [sys.executable, "-u", str(container["conf"] / "helper.py"),
            HDFS_OUT, str(container["creds"]), interval]


def _run_helper(container, stdin_bytes, interval="0.2"):
    return subprocess.run(_argv(container, interval), input=stdin_bytes, capture_output=True,
                          timeout=15, env=container["env"], cwd=str(container["conf"].parent))


def _popen_helper(container, interval="0.2"):
    return subprocess.Popen(_argv(container, interval), stdin=subprocess.PIPE,
                            stderr=subprocess.PIPE, env=container["env"],
                            cwd=str(container["conf"].parent))


def _wait_for(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return False


def test_e2e_writes_all_input_to_hdfs(container):
    stdin = b"line1\nline2\nline3\n"
    proc = _run_helper(container, stdin)
    assert proc.returncode == 0, proc.stderr.decode()
    assert container["server"].files[HDFS_OUT] == stdin
    assert container["server"].permissions[HDFS_OUT] == "600"
    assert container["buf"].read_bytes() == stdin


def test_e2e_empty_input_creates_empty_file(container):
    proc = _run_helper(container, b"")
    assert proc.returncode == 0, proc.stderr.decode()
    assert container["server"].files[HDFS_OUT] == b""


def test_e2e_preserves_partial_final_line(container):
    stdin = b"line1\nline2 (no newline)"
    proc = _run_helper(container, stdin)
    assert proc.returncode == 0, proc.stderr.decode()
    assert container["server"].files[HDFS_OUT] == stdin


def test_e2e_timer_flushes_without_new_input(container):
    """Bytes must land on HDFS while the connector is silent — before stdin
    closes — and later commits must append only the delta."""
    files = container["server"].files
    proc = _popen_helper(container, interval="0.2")
    try:
        proc.stdin.write(b"first\n")
        proc.stdin.flush()
        assert _wait_for(lambda: files.get(HDFS_OUT) == b"first\n"), \
            "timed commit did not land while stdin still open"

        proc.stdin.write(b"second\n")
        proc.stdin.flush()
        assert _wait_for(lambda: files.get(HDFS_OUT) == b"first\nsecond\n")
    finally:
        _, stderr = proc.communicate(timeout=15)  # closes stdin → EOF → final commit
    assert proc.returncode == 0, stderr.decode()
    assert files[HDFS_OUT] == b"first\nsecond\n"

    data_ops = [op for (_, path, op) in container["server"].requests
                if path.startswith("/dn/") and op in ("CREATE", "APPEND")]
    # One CREATE, then one APPEND per landed delta; nothing re-uploaded.
    assert data_ops == ["CREATE", "APPEND"]


def test_recovers_from_partial_write_against_real_client(webhdfs, tmp_path, capsys):
    """Real client + fake DataNode failing mid-write after persisting a
    prefix: the retry must resume from what landed — no gap, no duplicate."""
    webhdfs.fail_writes = [3]
    client = WebHdfsClient.from_yarn_config(webhdfs.yarn_config)
    a = HdfsAppender(client, HDFS_OUT, str(tmp_path / "buf"), str(tmp_path / "chunk"),
                     backoff=(0,), sleep=lambda _s: None)
    helper.run(io.BytesIO(b"abcdef\n"), a, interval=60)
    assert "HDFS commit failed" in capsys.readouterr().err
    assert webhdfs.files[HDFS_OUT] == b"abcdef\n"
    data_ops = [op for (_, path, op) in webhdfs.requests if path.startswith("/dn/")]
    assert data_ops == ["CREATE", "APPEND"]  # failed CREATE landed 3 bytes, APPEND sent the rest


def test_e2e_sigterm_commits_buffered_bytes(container):
    """docker stop / YARN kill grace period: SIGTERM must flush what is
    buffered, then exit non-zero."""
    files = container["server"].files
    proc = _popen_helper(container, interval="60")  # timer never fires
    try:
        proc.stdin.write(b"buffered\n")
        proc.stdin.flush()
        assert _wait_for(lambda: container["buf"].exists()
                         and container["buf"].read_bytes() == b"buffered\n")
        assert HDFS_OUT not in files
        proc.send_signal(signal.SIGTERM)
        _, stderr = proc.communicate(timeout=15)
    finally:
        if proc.poll() is None:
            proc.kill()
    assert proc.returncode == 128 + signal.SIGTERM, stderr.decode()
    assert files[HDFS_OUT] == b"buffered\n"


def test_bad_token_fails_loudly_against_real_client(webhdfs, tmp_path, capsys):
    """Wrong gateway token → 401 on every attempt → retries exhausted → raise."""
    client = WebHdfsClient(webhdfs.url, "Basic bad")
    a = HdfsAppender(client, HDFS_OUT, str(tmp_path / "buf"), str(tmp_path / "chunk"),
                     backoff=(0,), sleep=lambda _s: None)
    with pytest.raises(WebHdfsError) as exc:
        helper.run(io.BytesIO(b"x\n"), a, interval=60)
    assert exc.value.status == 401
    assert "HTTP 401" in capsys.readouterr().err
    assert HDFS_OUT not in webhdfs.files