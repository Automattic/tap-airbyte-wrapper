"""
In-process fake WebHDFS gateway for tests.

Implements the subset the tap uses (MKDIRS, CREATE, APPEND, GETFILESTATUS,
OPEN with offset, DELETE), including the NameNode -> DataNode 307 redirect
for the data-carrying ops and basic-auth checking, so both the client and
helper.py can be exercised end to end without Hadoop.
"""
import base64
import json
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

YARN_CONFIG = {"base_url": "https://gateway.example.com", "username": "u", "password": "p"}


class FakeWebHdfs:
    def __init__(self, username="u", password="p"):
        self.files = {}          # hdfs path -> bytes
        self.permissions = {}    # hdfs path -> permission string (files and dirs)
        self.requests = []       # (method, path_on_server, op)
        self.expected_auth = "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()
        # Failure injection for CREATE/APPEND on the DataNode hop: each entry
        # fails one write after storing that many bytes of its body.
        self.fail_writes = []
        self.lock = threading.Lock()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def url(self):
        host, port = self.server.server_address
        return f"http://{host}:{port}"

    @property
    def yarn_config(self):
        return {**YARN_CONFIG, "webhdfs_base_url": self.url}

    def start(self):
        self.thread.start()
        return self

    def stop(self):
        self.server.shutdown()
        self.server.server_close()

    def _handler(self):
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _reply(self, status, body=b"", headers=None):
                self.send_response(status)
                for k, v in (headers or {}).items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _json(self, status, payload):
                self._reply(status, json.dumps(payload).encode(), {"Content-Type": "application/json"})

            def _not_found(self):
                self._json(404, {"RemoteException": {"exception": "FileNotFoundException"}})

            def _handle(self):
                parsed = urllib.parse.urlparse(self.path)
                params = dict(urllib.parse.parse_qsl(parsed.query))
                op = params.get("op")
                fake.requests.append((self.command, parsed.path, op))
                if self.headers.get("Authorization") != fake.expected_auth:
                    return self._reply(401)
                on_datanode = parsed.path.startswith("/dn/webhdfs/v1")
                hdfs_path = parsed.path[len("/dn/webhdfs/v1") if on_datanode else len("/webhdfs/v1"):]
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))

                # NameNode hop: data ops are redirected to the "DataNode".
                if op in ("CREATE", "APPEND", "OPEN") and not on_datanode:
                    if body:
                        # Spec: two-step create/append exists so clients don't
                        # send data before the redirect. Treat it as a bug.
                        return self._json(400, {"RemoteException": {
                            "exception": "IllegalArgumentException",
                            "message": "body sent to NameNode hop"}})
                    if op in ("APPEND", "OPEN") and hdfs_path not in fake.files:
                        return self._not_found()
                    # Real NameNodes keep the query string on the redirect.
                    return self._reply(307, headers={"Location": f"{fake.url}/dn{self.path}"})

                with fake.lock:
                    if op == "MKDIRS":
                        fake.permissions[hdfs_path] = params.get("permission")
                        return self._json(200, {"boolean": True})
                    if op == "GETFILESTATUS":
                        if hdfs_path not in fake.files:
                            return self._not_found()
                        return self._json(200, {"FileStatus": {"length": len(fake.files[hdfs_path])}})
                    if op == "DELETE":
                        removed = [p for p in fake.files if p == hdfs_path
                                   or (params.get("recursive") == "true" and p.startswith(hdfs_path + "/"))]
                        for p in removed:
                            del fake.files[p]
                        return self._json(200, {"boolean": bool(removed)})
                    if op == "OPEN":
                        return self._reply(200, fake.files[hdfs_path][int(params.get("offset", 0)):])
                    if op in ("CREATE", "APPEND"):
                        if op == "CREATE" and params.get("overwrite") == "false" and hdfs_path in fake.files:
                            return self._json(403, {"RemoteException": {"exception": "FileAlreadyExistsException"}})
                        if fake.fail_writes:
                            landed = fake.fail_writes.pop(0)
                            base = fake.files.get(hdfs_path, b"") if op == "APPEND" else b""
                            fake.files[hdfs_path] = base + body[:landed]
                            return self._json(500, {"RemoteException": {"exception": "IOException"}})
                        if op == "CREATE":
                            fake.files[hdfs_path] = body
                            fake.permissions[hdfs_path] = params.get("permission")
                            return self._reply(201)
                        fake.files[hdfs_path] += body
                        return self._reply(200)
                return self._reply(400)

            do_GET = do_PUT = do_POST = do_DELETE = _handle

        return Handler


@pytest.fixture
def webhdfs():
    fake = FakeWebHdfs().start()
    yield fake
    fake.stop()