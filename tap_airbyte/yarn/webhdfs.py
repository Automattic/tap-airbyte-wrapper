"""
Minimal WebHDFS client — same gateway and basic-auth as the YARN REST API:
{base_url}/webhdfs/v1/<path>?op=... Set `webhdfs_base_url` in
yarn_service_config if WebHDFS lives on a different endpoint.

Protocol reference (ops, params, status codes, redirect flow):
https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-hdfs/WebHDFS.html
Error mapping used here: FileNotFoundException -> 404, IOException (incl.
FileAlreadyExistsException, AlreadyBeingCreatedException) -> 403.

Keep this module stdlib-only and free of package imports: it is uploaded
into the Airbyte container next to helper.py and imported there as a bare
module, so the same client serves both sides (Meltano and the in-container
relay).

TLS: the gateway certificate is verified against the system CA store of
whichever image runs the client.
"""
import base64
import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional

logger = logging.getLogger(__name__)

REDIRECT_CODES = {301, 302, 307}


class WebHdfsError(Exception):
    def __init__(self, method, url, status, body):
        self.status = status
        super().__init__(f"{method} {url} -> HTTP {status}: {body[:500]!r}")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Surface redirects instead of following them: urllib turns a 307 POST/PUT
    into a bodiless retry (or an error), and WebHDFS needs the body re-sent to
    the DataNode the NameNode points at."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # pylint: disable=too-many-arguments
        return None


class WebHdfsClient:
    """Holds only the ready-made `Authorization` header, never the raw
    username/password, so what gets shipped to the Airbyte container
    (`credentials_json`) is the Basic token — still a secret, but not
    plaintext credentials lying around in a file."""

    def __init__(self, base_url, authorization, extra_headers=None, timeout=120):
        self.base_url = base_url.rstrip("/")
        self.authorization = authorization
        self.extra_headers = dict(extra_headers or {})
        self.timeout = timeout
        self.headers = {"Authorization": authorization,
                        "Content-Type": "application/octet-stream",
                        **self.extra_headers}
        self._opener = urllib.request.build_opener(_NoRedirect())

    @staticmethod
    def basic_auth(username, password) -> str:
        return "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()

    @classmethod
    def from_yarn_config(cls, yarn_config):
        return cls(
            yarn_config.get("webhdfs_base_url") or yarn_config["base_url"],
            cls.basic_auth(yarn_config["username"], yarn_config["password"]),
            yarn_config.get("extra_headers"),
        )

    def credentials_json(self) -> str:
        """Serialized constructor args, shipped to the container for helper.py."""
        return json.dumps({
            "base_url": self.base_url,
            "authorization": self.authorization,
            "extra_headers": self.extra_headers,
        })

    @classmethod
    def from_credentials_file(cls, path):
        with open(path, "r", encoding="utf-8") as f:
            return cls(**json.load(f))

    # -- transport ----------------------------------------------------------

    def _http(self, method, url, data=None, from_file=None):
        """One HTTP round-trip. Body is `data` (bytes) or the contents of
        `from_file` (a local path, re-opened per attempt so the body can be
        re-sent on redirect without holding it in memory). Returns
        (status, headers, body); HTTP errors are returned, not raised."""
        headers = dict(self.headers)
        body = data
        if from_file is not None:
            body = open(from_file, "rb")  # pylint: disable=consider-using-with
            headers["Content-Length"] = str(os.fstat(body.fileno()).st_size)
        try:
            req = urllib.request.Request(url, data=body, method=method, headers=headers)
            try:
                with self._opener.open(req, timeout=self.timeout) as resp:
                    return resp.status, resp.headers, resp.read()
            except urllib.error.HTTPError as exc:
                return exc.code, exc.headers, exc.read()
        finally:
            if from_file is not None:
                body.close()

    def _request(self, method, hdfs_path, op, params=None, data=None, from_file=None):
        """Two-step protocol per the WebHDFS spec: data-carrying ops (CREATE,
        APPEND) hit the NameNode *without* a body, get a 307 to a DataNode,
        and only then upload — otherwise the payload would travel through
        the gateway twice (the NameNode just discards it)."""
        query = urllib.parse.urlencode({"op": op, **(params or {})})
        url = f"{self.base_url}/webhdfs/v1{urllib.parse.quote(hdfs_path)}?{query}"
        has_body = data or from_file is not None
        status, headers, body = self._http(method, url)
        if status in REDIRECT_CODES:
            url = headers["Location"]
            status, headers, body = self._http(method, url, data, from_file)
        elif has_body and status < 400:
            raise WebHdfsError(method, url, status,
                               b"expected a redirect to a DataNode before uploading the body")
        return status, body, url

    def _check(self, method, url, status, body, ok=(200, 201)):
        if status not in ok:
            raise WebHdfsError(method, url, status, body)

    # -- operations ---------------------------------------------------------

    def write_file(self, hdfs_path, data=b"", permission="600", overwrite=True, from_file=None):
        """Create (or overwrite) a file from `data` (bytes/str) or the local
        file `from_file`. Defaults to owner-only permission — uploads carry
        connector credentials."""
        if isinstance(data, str):
            data = data.encode("utf-8")
        status, body, url = self._request(
            "PUT", hdfs_path, "CREATE",
            {"overwrite": str(overwrite).lower(), "permission": permission},
            data=data, from_file=from_file,
        )
        self._check("PUT", url, status, body)

    def append_file(self, hdfs_path, data=b"", from_file=None):
        """Append `data` (bytes) or the local file `from_file` to an existing file."""
        status, body, url = self._request("POST", hdfs_path, "APPEND", data=data, from_file=from_file)
        self._check("POST", url, status, body)

    def mkdirs(self, hdfs_path, permission="700"):
        """Create a directory (and parents), owner-only by default."""
        status, body, url = self._request("PUT", hdfs_path, "MKDIRS", {"permission": permission})
        self._check("PUT", url, status, body)

    def file_length(self, hdfs_path) -> Optional[int]:
        """File length in bytes, or None if the file doesn't exist."""
        status, body, url = self._request("GET", hdfs_path, "GETFILESTATUS")
        if status == 404:
            return None
        self._check("GET", url, status, body)
        return json.loads(body)["FileStatus"]["length"]

    def read_file(self, hdfs_path, offset=0) -> bytes:
        """File content from the byte offset to EOF; b"" if the file is missing."""
        status, body, url = self._request("GET", hdfs_path, "OPEN", {"offset": str(offset)})
        if status == 404:
            # Not created yet (helper's first commit hasn't landed)
            return b""
        self._check("GET", url, status, body)
        return body

    def delete(self, hdfs_path, recursive=False):
        """Best-effort delete; failures are logged, not raised."""
        try:
            self._request("DELETE", hdfs_path, "DELETE", {"recursive": str(recursive).lower()})
        except Exception:  # pylint: disable=broad-except
            logger.warning("Failed to delete %s from HDFS", hdfs_path, exc_info=True)


# -- yarn_config-based convenience wrappers (Meltano side) -------------------

def hdfs_write_file(yarn_config, hdfs_path, content, permission="600"):
    WebHdfsClient.from_yarn_config(yarn_config).write_file(hdfs_path, content, permission)


def hdfs_mkdirs(yarn_config, hdfs_path, permission="700"):
    WebHdfsClient.from_yarn_config(yarn_config).mkdirs(hdfs_path, permission)


def hdfs_file_length(yarn_config, hdfs_path) -> Optional[int]:
    return WebHdfsClient.from_yarn_config(yarn_config).file_length(hdfs_path)


def hdfs_read_file(yarn_config, hdfs_path, offset=0) -> bytes:
    return WebHdfsClient.from_yarn_config(yarn_config).read_file(hdfs_path, offset)


def hdfs_delete(yarn_config, hdfs_path, recursive=False):
    WebHdfsClient.from_yarn_config(yarn_config).delete(hdfs_path, recursive)