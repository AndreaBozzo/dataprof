"""A complete async scan reports ``bytes_consumed`` equal to the source's length (#1223).

``profile_url`` passes the response's ``Content-Length`` as the size hint, and a
server that sends none (a chunked or close-delimited body) left the report to
sum the readers' per-chunk tallies. The JSON readers derived theirs from the
records they parsed, so a 17-byte JSONL body reported 26; the CSV reader left
out the blank lines after its last record.

``tests/async_bytes_consumed.rs`` is the Rust twin; it also covers streams
without a hint, one-byte chunks and a row cap.
"""

from __future__ import annotations

import asyncio
import http.server
import socketserver
import threading
from collections.abc import Iterator

import pytest

try:
    import dataprof as dp
    from dataprof.asyncio import _HAS_ASYNC, _HAS_URL, profile_bytes, profile_url
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

pytestmark = pytest.mark.skipif(not _HAS_ASYNC, reason="async support not compiled")

#: case -> (format, payload). Each holds bytes no record carries.
PAYLOADS = {
    "jsonl, blank line": ("jsonl", b'{"a":1}\n\n{"a":2}\n'),
    "jsonl, BOM": ("jsonl", b'\xef\xbb\xbf{"a":1}\n{"a":2}\n'),
    "jsonl, malformed line": ("jsonl", b'{"a":1}\n{bad\n{"a":2}\n'),
    "json array, white space": ("json", b'[ {"a":1} , {"a":2} ]\n'),
    "json array, non-object element": ("json", b'[1, {"a":2}, {"a":3}]'),
    "json object": ("json", b'  {"a": 1}  \n'),
    "csv": ("csv", b"a,b\n1,2\n3,4\n"),
    "csv, trailing blank lines": ("csv", b"a,b\n1,2\n3,4\n\n\n"),
}


class _Server(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


@pytest.fixture()
def serve() -> Iterator[tuple[str, dict[str, bytes]]]:
    """The base URL, and the routes it serves: with ``Content-Length``, unless
    the path starts with ``/close/``."""
    routes: dict[str, bytes] = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.0"  # a body without a length ends at close

        def do_GET(self) -> None:
            payload = routes[self.path]
            self.send_response(200)
            if not self.path.startswith("/close/"):
                self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:
            return

    with _Server(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_address[1]}", routes
        finally:
            server.shutdown()


def _consumed(report: dp.ProfileReport) -> tuple[object, object]:
    execution = report.to_dict()["execution"]
    return execution.get("bytes_consumed"), execution.get("source_exhausted")


@pytest.mark.skipif(not _HAS_URL, reason="async URL profiling not compiled")
def test_a_complete_async_scan_consumes_the_whole_source(
    serve: tuple[str, dict[str, bytes]],
) -> None:
    base, served = serve
    wrong = {}
    for index, (case, (fmt, payload)) in enumerate(PAYLOADS.items()):
        for framing in ("length", "close"):
            path = f"/{framing}/{index}.{fmt}"
            served[path] = payload
        routes = {
            "profile_bytes": asyncio.run(profile_bytes(payload, format=fmt)),
            "url with Content-Length": asyncio.run(profile_url(f"{base}/length/{index}.{fmt}")),
            "url without Content-Length": asyncio.run(profile_url(f"{base}/close/{index}.{fmt}")),
        }
        for route, report in routes.items():
            got = _consumed(report)
            if got != (len(payload), True):
                wrong[f"{case}, {route}"] = (got, len(payload))
    assert not wrong, wrong
