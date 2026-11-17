# Copyright (c) 2022 Alex Butler
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
# to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or
# substantial portions of the Software.

"""Tests for the ABORT message emitted when a sync fails part-way."""

import io
import json
import signal
import subprocess
import sys
import textwrap
import types

import pytest
from singer_sdk import typing as th
from singer_sdk.sinks import BatchSink
from singer_sdk.target_base import Target

from tap_airbyte import tap as tap_module
from tap_airbyte.tap import (
    MAX_ABORT_REASON_CHARS,
    PIPE_CLOSED,
    TapAirbyte,
    abort_message,
    abort_on_signal,
    write_abort_message,
)


@pytest.fixture(autouse=True)
def _reset_abort_flag():
    tap_module._abort_written = False
    yield
    tap_module._abort_written = False

SCHEMA = json.dumps(
    {
        "type": "SCHEMA",
        "stream": "contacts",
        "schema": {"properties": {"id": {"type": "integer"}}},
        "key_properties": ["id"],
    }
) + "\n"
RECORD = json.dumps({"type": "RECORD", "stream": "contacts", "record": {"id": 1}}) + "\n"


def _buffering_target(committed: list):
    """A target that only commits in clean_up(), like target-iceberg in overwrite mode."""

    class BufferingSink(BatchSink):
        max_size = 1000

        def process_batch(self, context) -> None:
            pass  # buffer only; the real write happens in clean_up()

        def clean_up(self) -> None:
            committed.append(self.stream_name)

    class BufferingTarget(Target):
        name = "buffering-target"
        config_jsonschema = th.PropertiesList().to_dict()
        default_sink_class = BufferingSink

    return BufferingTarget(config={}, validate_config=False)


def test_clean_eof_lets_target_commit():
    """Control case: without an ABORT, EOF alone makes the target commit."""
    committed: list = []
    _buffering_target(committed).listen(io.StringIO(SCHEMA + RECORD))
    assert committed == ["contacts"]


def test_abort_message_stops_target_before_it_commits():
    """The whole point: an ABORT line must stop the target before clean_up() runs.

    Guards the undocumented SDK behaviour this relies on -- an unrecognized message
    type raises while reading lines, so end-of-pipe handling never runs.
    """
    committed: list = []
    target = _buffering_target(committed)

    reason = "AirbyteException: Airbyte process failed with return code 1"
    payload = abort_message(reason).decode()
    assert reason in payload, "reason should travel with the message"

    with pytest.raises(ValueError, match="ABORT"):
        target.listen(io.StringIO(SCHEMA + RECORD + payload))

    assert committed == []


def test_sync_all_writes_abort_and_reraises(monkeypatch):
    """A failing sync must emit ABORT and still propagate the original error."""
    calls: list = []
    monkeypatch.setattr(tap_module, "write_abort_message", lambda reason="": calls.append(reason))

    class AirbyteDied(Exception):
        pass

    def boom(self) -> None:
        raise AirbyteDied("Airbyte process failed with return code 1")

    fake_tap = types.SimpleNamespace(_sync_streams=types.MethodType(boom, object()))

    with pytest.raises(AirbyteDied):
        TapAirbyte.sync_all(fake_tap)

    assert calls == ["AirbyteDied: Airbyte process failed with return code 1"]


def test_sync_all_writes_no_abort_on_success(monkeypatch):
    calls: list = []
    monkeypatch.setattr(tap_module, "write_abort_message", lambda reason="": calls.append(reason))

    fake_tap = types.SimpleNamespace(_sync_streams=lambda: None)
    TapAirbyte.sync_all(fake_tap)

    assert calls == []


def test_write_abort_message_emits_sentinel(monkeypatch):
    monkeypatch.setattr(TapAirbyte, "pipe_status", None)
    buffer = io.BytesIO()
    monkeypatch.setattr(tap_module.sys, "stdout", types.SimpleNamespace(buffer=buffer))

    write_abort_message()

    assert buffer.getvalue() == abort_message()
    assert buffer.getvalue().endswith(b"\n"), "target reads line by line"


def test_write_abort_message_skipped_when_pipe_closed(monkeypatch):
    """Nothing to tell a target that has already gone away, and writing would EPIPE."""
    monkeypatch.setattr(TapAirbyte, "pipe_status", PIPE_CLOSED)
    buffer = io.BytesIO()
    monkeypatch.setattr(tap_module.sys, "stdout", types.SimpleNamespace(buffer=buffer))

    write_abort_message()

    assert buffer.getvalue() == b""


def test_write_abort_message_is_idempotent(monkeypatch):
    """The signal handler and sync_all's except clause can both fire; emit once."""
    monkeypatch.setattr(TapAirbyte, "pipe_status", None)
    buffer = io.BytesIO()
    monkeypatch.setattr(tap_module.sys, "stdout", types.SimpleNamespace(buffer=buffer))

    write_abort_message("first")
    write_abort_message("second")

    assert buffer.getvalue() == abort_message("first")


def test_abort_on_signal_emits_and_exits(monkeypatch):
    monkeypatch.setattr(TapAirbyte, "pipe_status", None)
    buffer = io.BytesIO()
    monkeypatch.setattr(tap_module.sys, "stdout", types.SimpleNamespace(buffer=buffer))

    with pytest.raises(SystemExit) as excinfo:
        abort_on_signal(signal.SIGTERM, None)

    assert excinfo.value.code == 128 + signal.SIGTERM
    assert json.loads(buffer.getvalue())["reason"] == "terminated by signal SIGTERM"


def test_sigterm_during_sync_emits_abort(tmp_path):
    """SIGTERM terminates Python without unwinding, so cover it end to end.

    YARN container kills and Airflow task kills send SIGTERM before SIGKILL, so
    this is the realistic abrupt-death path -- not a theoretical one.
    """
    child = tmp_path / "child.py"
    child.write_text(textwrap.dedent("""
        import sys, time, types
        from tap_airbyte.tap import TapAirbyte

        def block(self):
            print("ready", file=sys.stderr, flush=True)
            time.sleep(60)

        TapAirbyte.sync_all(types.SimpleNamespace(
            _sync_streams=types.MethodType(block, object())
        ))
    """))

    proc = subprocess.Popen(
        [sys.executable, str(child)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stderr.readline() == b"ready\n", "child never started syncing"
        proc.send_signal(signal.SIGTERM)
        stdout, _ = proc.communicate(timeout=30)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.communicate()

    message = json.loads(stdout)
    assert message["type"] == "ABORT", f"expected ABORT on stdout, got {stdout!r}"
    assert message["reason"] == "terminated by signal SIGTERM"
    assert proc.returncode == 128 + signal.SIGTERM


def test_abort_reason_is_truncated():
    """AirbyteException embeds connector stderr; an untruncated line would trip
    Meltano's line-length limit instead of aborting the target."""
    message = json.loads(abort_message("x" * (MAX_ABORT_REASON_CHARS * 3)))

    assert len(message["reason"]) == MAX_ABORT_REASON_CHARS
