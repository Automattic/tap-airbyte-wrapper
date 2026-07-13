"""
WebHDFS client — same gateway and basic-auth as the YARN REST API:
{base_url}/webhdfs/v1/<path>?op=... Set `webhdfs_base_url` in
yarn_service_config if WebHDFS lives on a different endpoint.
"""
import logging
from typing import Optional

from tap_airbyte.yarn.session import YarnConfig, create_session

logger = logging.getLogger(__name__)


def _webhdfs_request(yarn_config: YarnConfig, method: str, hdfs_path: str, op: str,
                     params: Optional[dict] = None, data: Optional[bytes] = None):
    """
    Issue a WebHDFS request, following the NameNode->DataNode 307 redirect
    manually so the redirect target is hit with the same body.
    """
    session = create_session(yarn_config)
    base_url = yarn_config.get('webhdfs_base_url', yarn_config['base_url']).rstrip('/')
    url = f"{base_url}/webhdfs/v1{hdfs_path}"
    kwargs = {
        "params": {"op": op, **(params or {})},
        "data": data,
        "headers": {"Content-Type": "application/octet-stream"},
    }
    response = session.request(method, url, allow_redirects=False, **kwargs)
    if response.status_code in {301, 302, 307}:
        response = session.request(method, response.headers["Location"], **kwargs)
    return response


def hdfs_write_file(yarn_config: YarnConfig, hdfs_path: str, content) -> None:
    """Create (or overwrite) an HDFS file with the given content."""
    if isinstance(content, str):
        content = content.encode("utf-8")
    response = _webhdfs_request(
        yarn_config, "PUT", hdfs_path, "CREATE", {"overwrite": "true"}, data=content
    )
    response.raise_for_status()


def hdfs_file_length(yarn_config: YarnConfig, hdfs_path: str) -> Optional[int]:
    """Return the file length in bytes, or None if the file doesn't exist."""
    response = _webhdfs_request(yarn_config, "GET", hdfs_path, "GETFILESTATUS")
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()["FileStatus"]["length"]


def hdfs_read_file(yarn_config: YarnConfig, hdfs_path: str, offset: int = 0) -> bytes:
    """Read the file content from the given byte offset to EOF."""
    response = _webhdfs_request(yarn_config, "GET", hdfs_path, "OPEN", {"offset": str(offset)})
    if response.status_code == 404:
        # File replaced mid-commit (`hdfs dfs -put -f` deletes + recreates)
        return b""
    response.raise_for_status()
    return response.content


def hdfs_delete(yarn_config: YarnConfig, hdfs_path: str, recursive: bool = False) -> None:
    """Best-effort delete; failures are logged, not raised."""
    try:
        _webhdfs_request(
            yarn_config, "DELETE", hdfs_path, "DELETE", {"recursive": str(recursive).lower()}
        )
    except Exception:  # pylint: disable=broad-except
        logger.warning("Failed to delete %s from HDFS", hdfs_path, exc_info=True)