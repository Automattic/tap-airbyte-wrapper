"""
Stdout relay that runs *inside* the Airbyte container.

Usage: python -u helper.py <hdfs_path> <webhdfs_credentials.json> [commit_interval_seconds]

Reads main.py's stdout on stdin, buffers it on local disk, and ships it to
HDFS over WebHDFS (CREATE on the first commit, APPEND after that). Every
commit interval (default 20s) only the bytes written since the last commit
are sent — a timer, not input arrival, drives commits, so a quiet connector
still gets its buffered lines flushed, and transfer volume is O(n) rather
than re-uploading the whole file each time. The HDFS file is append-only, so
byte offsets stay valid across commits and the Meltano side follows it with
WebHDFS offset reads.

A failed or hung commit is retried. HDFS may have kept a partial write, so
before each retry the remote length is checked and any bytes already landed
are skipped, avoiding duplicated records. Backoff totals >60s to outlive the
lease soft limit left behind by a crashed append client. SIGTERM (docker
stop / YARN kill grace period) triggers one last commit.

No Hadoop CLI/JVM needed in the image: only python (3.9+ assumed) and
network access to the WebHDFS gateway. Keep this file stdlib-only: it is
uploaded verbatim together with webhdfs.py to the per-run HDFS dir and
localized into the container.
"""
import os
import signal
import sys
import threading
import time

try:
    from tap_airbyte.yarn.webhdfs import WebHdfsClient, WebHdfsError
except ImportError:  # running as a bare script inside the container
    from webhdfs import WebHdfsClient, WebHdfsError

# Hand-rolled retry (no tenacity): nothing is pip-installed in the Airbyte
# image, this file must stay stdlib-only (see module docstring).
RETRY_BACKOFF = (10, 20, 40)
DEFAULT_COMMIT_INTERVAL = 20.0
CHUNK_SIZE = 1 << 20
COMMIT_ERRORS = (WebHdfsError, OSError)  # OSError covers URLError/timeouts


class HdfsAppender:
    """Append-only local buffer with incremental commits to an HDFS file.

    The local file is unbuffered, so `written` (bumped by the writer thread
    after each successful syscall) only ever counts bytes already visible to
    a reader — commit() can snapshot it without any locking.
    """

    def __init__(self, client, hdfs_path, local_path, chunk_path,
                 backoff=RETRY_BACKOFF, sleep=time.sleep):
        self.client = client
        self.hdfs_path = hdfs_path
        self.local_path = local_path
        self.chunk_path = chunk_path
        self.backoff = tuple(backoff)
        self.sleep = sleep
        self.buf = open(local_path, "wb", buffering=0)  # pylint: disable=consider-using-with
        self.written = 0
        self.committed = 0
        self.created = False

    def write(self, data):
        view = memoryview(data)
        while view:
            n = self.buf.write(view)
            self.written += n
            view = view[n:]

    def close(self):
        self.buf.close()

    def remote_length(self):
        """Length of the HDFS file, or 0 if missing/unreachable."""
        try:
            length = self.client.file_length(self.hdfs_path)
        except COMMIT_ERRORS:
            return 0
        if length is None:
            self.created = False
            return 0
        self.created = True
        return length

    def _write_chunk(self, start, end):
        with open(self.local_path, "rb") as src, open(self.chunk_path, "wb") as dst:
            src.seek(start)
            remaining = end - start
            while remaining > 0:
                data = src.read(min(remaining, CHUNK_SIZE))
                if not data:
                    raise IOError(f"local buffer shorter than expected ({end} bytes)")
                dst.write(data)
                remaining -= len(data)

    def _send_chunk(self):
        if self.created:
            self.client.append_file(self.hdfs_path, from_file=self.chunk_path)
        else:
            self.client.write_file(self.hdfs_path, from_file=self.chunk_path,
                                   permission="600", overwrite=False)
            self.created = True

    def commit(self, final=False):
        """Send everything buffered since the last commit to HDFS."""
        end = self.written
        # Skip the round-trip when there is nothing new; the final commit
        # always runs so an empty stdout still yields a file on HDFS.
        if end == self.committed and not (final and not self.created):
            return
        self._write_chunk(self.committed, end)
        for attempt, backoff in enumerate(self.backoff + (None,)):
            try:
                self._send_chunk()
                break
            except COMMIT_ERRORS as exc:
                if backoff is None:
                    raise
                print(f"HDFS commit failed (attempt {attempt + 1}: {exc}), retrying in {backoff}s",
                      file=sys.stderr, flush=True)
                self.sleep(backoff)
                # Part of the chunk may have landed before the failure.
                landed = max(self.remote_length(), self.committed)
                if landed >= end:
                    break
                self._write_chunk(landed, end)
        self.committed = end


def drain(stream, appender, closed):
    """Copy stream into the appender until EOF, then signal `closed`."""
    try:
        while True:
            data = stream.read1(CHUNK_SIZE)
            if not data:
                break
            appender.write(data)
    finally:
        closed.set()


def run(stream, appender, interval):
    """Drive timed commits until the stream closes, then do the final commit."""
    stdin_closed = threading.Event()
    reader = threading.Thread(target=drain, args=(stream, appender, stdin_closed), daemon=True)
    reader.start()
    try:
        while not stdin_closed.wait(interval):
            appender.commit()
        reader.join()
    finally:
        try:
            appender.commit(final=True)
        finally:
            appender.close()


def main(argv):
    hdfs_path = argv[1]
    client = WebHdfsClient.from_credentials_file(argv[2])
    interval = float(argv[3]) if len(argv) > 3 else DEFAULT_COMMIT_INTERVAL
    buf_dir = os.environ.get("AIRBYTE_HELPER_BUF_DIR", "/tmp")

    def on_sigterm(signum, _frame):
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, on_sigterm)
    appender = HdfsAppender(
        client,
        hdfs_path,
        os.path.join(buf_dir, "airbyte_buf"),
        os.path.join(buf_dir, "airbyte_chunk"),
    )
    run(sys.stdin.buffer, appender, interval)


if __name__ == "__main__":
    main(sys.argv)
