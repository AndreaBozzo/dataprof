"""A report records the source it read on every route (#1219).

``dataprof.asyncio.profile_bytes`` recorded an in-memory buffer as an HTTP
stream, because report assembly wrote ``http`` for any stream whose caller
declared no system. Bytes now record the same ``bytes`` source on the sync and
async routes; a file and a URL keep the system their route declares.

``tests/async_source_provenance.rs`` is the Rust twin.
"""

from __future__ import annotations

import asyncio
import http.server
import json
import socketserver
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

try:
    import dataprof as dp
    from dataprof.asyncio import _HAS_URL, profile_bytes, profile_file, profile_url
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PARQUET = (REPO_ROOT / "examples" / "test_data" / "simple.parquet").read_bytes()

#: format -> bytes of that format
PAYLOADS = {
    "csv": b"a,b\n1,2\n3,4\n",
    "json": b'[{"a":1},{"a":2}]',
    "jsonl": b'{"a":1}\n{"a":2}\n',
    "parquet": PARQUET,
}


def _source(report: dp.ProfileReport) -> dict[str, Any]:
    source = json.loads(report.to_json())["data_source"]
    if "batch_id" in source:
        # A fresh UUID per run; its presence is pinned, its value cannot be.
        source["batch_id"] = "<uuid>"
    return source


def test_bytes_record_the_same_source_on_both_routes() -> None:
    wrong = {}
    for fmt, data in PAYLOADS.items():
        sync = dp.profile(data, format=fmt)
        async_ = asyncio.run(profile_bytes(data, format=fmt))
        expected = {"type": "bytes", "name": f"{fmt}_bytes", "format": fmt}
        for route, report in (("sync", sync), ("async", async_)):
            source = _source(report)
            core = {key: source.get(key) for key in expected}
            if core != expected or source.get("size_bytes") != len(data):
                wrong[f"{fmt} {route}"] = source
        if _source(sync) != _source(async_):
            wrong[f"{fmt} sync vs async"] = (_source(sync), _source(async_))
        if sync.source != async_.source:
            wrong[f"{fmt} report.source"] = (sync.source, async_.source)
    assert not wrong, wrong


def test_async_file_records_a_file_stream(tmp_path: Path) -> None:
    path = tmp_path / "probe.csv"
    path.write_bytes(PAYLOADS["csv"])
    source = _source(asyncio.run(profile_file(path)))
    assert source["type"] == "stream"
    assert source["source_system"] == "file"
    assert source["topic"] == str(path)


class _Server(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


@pytest.fixture()
def csv_url() -> Iterator[str]:
    payload = PAYLOADS["csv"]

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:
            return

    with _Server(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_address[1]}/data.csv"
        finally:
            server.shutdown()


@pytest.mark.skipif(not _HAS_URL, reason="async URL profiling not compiled")
def test_url_records_an_http_stream(csv_url: str) -> None:
    source = _source(asyncio.run(profile_url(csv_url)))
    assert source["type"] == "stream"
    assert source["source_system"] == "http"
    assert source["topic"] == csv_url
