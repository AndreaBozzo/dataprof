"""A remote Parquet object replaced between ranged reads fails the read (#865).

The remote reader takes the size from one request and reads the footer and each
column chunk with separate ranged GETs. An object rewritten in between with the
same layout used to be profiled as a mix of both versions. The reader now pins
the object's ``ETag``: ``If-Match`` on every range, and the ``ETag`` of every
``206`` answer compared with the pinned one.

``tests/remote_parquet_versions.rs`` is the Rust twin.
"""

from __future__ import annotations

import asyncio
import contextlib
import http.server
import io
import socketserver
import threading

import dataprof
import pytest
from dataprof.asyncio import profile_url

pa = pytest.importorskip("pyarrow", reason="pyarrow writes the two versions")
pq = pytest.importorskip("pyarrow.parquet", reason="pyarrow writes the two versions")

if not dataprof.capabilities().remote_parquet:
    pytest.skip(
        "Remote Parquet profiling requires the parquet-async feature", allow_module_level=True
    )

ROWS = 10_000
#: HEAD, footer length, footer, then two column chunks in each of four row groups.
REQUESTS = 11


def _version(offset: int) -> bytes:
    values = list(range(offset, offset + ROWS))
    table = pa.table({"a": pa.array(values, pa.int64()), "b": pa.array([float(v) for v in values])})
    buffer = io.BytesIO()
    pq.write_table(
        table,
        buffer,
        compression="none",
        use_dictionary=False,
        column_encoding="PLAIN",
        write_statistics=False,
        row_group_size=ROWS // 4,
    )
    return buffer.getvalue()


A, B = _version(0), _version(1_000_000)


class _Server(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


@contextlib.contextmanager
def _swap_server(swap_after: int, mode: str):
    """Serve A for the first ``swap_after`` requests and B after.

    ``if_match_only``: an ``ETag`` on HEAD only, and 412 when ``If-Match`` names
    another version. ``etag_only``: an ``ETag`` on every answer, ``If-Match``
    ignored. ``none``: no ``ETag``.
    """
    lock = threading.Lock()
    served = [0]

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _version(self):
            with lock:
                index = served[0]
                served[0] += 1
            return (A, '"v-A"') if index < swap_after else (B, '"v-B"')

        def _etag(self, etag, head):
            if mode == "etag_only" or (mode == "if_match_only" and head):
                self.send_header("ETag", etag)

        def do_HEAD(self):
            data, etag = self._version()
            self.send_response(200)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Accept-Ranges", "bytes")
            self._etag(etag, head=True)
            self.end_headers()

        def do_GET(self):
            data, etag = self._version()
            if_match = self.headers.get("If-Match")
            if mode == "if_match_only" and if_match is not None and if_match != etag:
                self.send_response(412)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            start, _, end = self.headers["Range"].removeprefix("bytes=").partition("-")
            chunk = data[int(start) : int(end) + 1]
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
            self.send_header("Content-Length", str(len(chunk)))
            self._etag(etag, head=False)
            self.end_headers()
            self.wfile.write(chunk)

        def log_message(self, format, *args):
            return

    server = _Server(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/data.parquet", served
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _summary(report) -> tuple:
    a = report["a"]
    return (a.min, a.max, a.mean)


def test_a_replaced_object_fails_the_read_at_every_mixing_swap_point():
    wrong = []
    for mode in ("if_match_only", "etag_only"):
        for swap_after in range(REQUESTS + 1):
            with _swap_server(swap_after, mode) as (url, _):
                case = f"{mode}, replaced after request {swap_after}"
                try:
                    report = asyncio.run(profile_url(url))
                except OSError as error:
                    if swap_after in (0, REQUESTS):
                        wrong.append(f"{case}: {error}")
                    elif url not in str(error):
                        wrong.append(f"{case}: error does not name the URL: {error}")
                    continue
                expected = (
                    (1_000_000, 1_009_999, 1_004_999.5) if swap_after == 0 else (0, 9_999, 4_999.5)
                )
                if swap_after not in (0, REQUESTS) or _summary(report) != expected:
                    wrong.append(f"{case}: profiled as {_summary(report)}")
    assert not wrong, "\n".join(wrong)


@pytest.mark.parametrize("mode", ["if_match_only", "etag_only", "none"])
def test_an_unchanged_object_takes_the_same_requests(mode):
    with _swap_server(REQUESTS * 10, mode) as (url, served):
        report = asyncio.run(profile_url(url))
        assert _summary(report) == (0, 9_999, 4_999.5)
        assert served[0] == REQUESTS
