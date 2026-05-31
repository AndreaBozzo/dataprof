"""Async URL profiling regression tests.

These tests require building with async URL features:
    uv run maturin develop --features "python,python-async,async-streaming"

For remote Parquet coverage:
    uv run maturin develop --features "python,python-async,parquet-async"
"""

from __future__ import annotations

import asyncio
import contextlib
import http.server
import socketserver
import threading
from pathlib import Path

import pytest
from dataprof.asyncio import (
    _HAS_URL,
    infer_schema_stream,
    profile_bytes,
    profile_file,
    profile_url,
    quick_row_count_stream,
)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
FIXTURES = REPO_ROOT / "examples"
DOGFOOD = REPO_ROOT / "python" / "tests" / "fixtures" / "dogfood"
INCIDENTS_CSV = DOGFOOD / "incidents.csv"
CHECKOUT_JSONL = DOGFOOD / "checkout_events.jsonl"
PARQUET_FILE = FIXTURES / "test_data" / "simple.parquet"


if not _HAS_URL:
    pytest.skip(
        "Async URL profiling not compiled. Build with --features "
        "'python,python-async,async-streaming'.",
        allow_module_level=True,
    )


def _run(async_fn, *args, **kwargs):
    async def _inner():
        return await async_fn(*args, **kwargs)

    return asyncio.run(_inner())


class _ThreadingTcpServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


@pytest.fixture()
def url_server():
    incidents_csv = INCIDENTS_CSV.read_bytes()
    checkout_jsonl = CHECKOUT_JSONL.read_bytes()
    parquet_bytes = PARQUET_FILE.read_bytes()

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_HEAD(self):
            payload = self._payload()
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()

        def do_GET(self):
            payload = self._payload()
            range_header = self.headers.get("Range")
            if range_header:
                unit, _, byte_range = range_header.partition("=")
                assert unit == "bytes", f"unexpected range unit: {range_header}"
                start_str, _, end_str = byte_range.partition("-")
                start = int(start_str)
                end = int(end_str) if end_str else len(payload) - 1
                chunk = payload[start : end + 1]

                self.send_response(206)
                self.send_header("Content-Length", str(len(chunk)))
                self.send_header(
                    "Content-Range", f"bytes {start}-{start + len(chunk) - 1}/{len(payload)}"
                )
                self.send_header("Accept-Ranges", "bytes")
                self.end_headers()
                self.wfile.write(chunk)
                return

            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format, *args):
            return

        def _payload(self):
            if self.path == "/incidents.csv":
                return incidents_csv
            if self.path == "/checkout_events.jsonl":
                return checkout_jsonl
            if self.path == "/data.parquet":
                return parquet_bytes
            raise AssertionError(f"unexpected path: {self.path}")

    server = _ThreadingTcpServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        port = server.server_address[1]
        yield {
            "csv": f"http://127.0.0.1:{port}/incidents.csv",
            "jsonl": f"http://127.0.0.1:{port}/checkout_events.jsonl",
            "parquet": f"http://127.0.0.1:{port}/data.parquet",
        }
    finally:
        with contextlib.suppress(Exception):
            server.shutdown()
        server.server_close()
        thread.join(timeout=2)


class TestAsyncUrlProfiling:
    def test_profile_file_with_dogfood_csv(self):
        report = _run(profile_file, INCIDENTS_CSV)

        assert report.rows == 30
        assert report.columns == 12
        assert report["email"].null_count == 3
        assert report["sla_breached"].true_count == 12

    def test_profile_bytes_with_dogfood_jsonl(self):
        report = _run(profile_bytes, CHECKOUT_JSONL.read_bytes(), format="jsonl")

        assert report.rows == 25
        assert report.columns == 10
        assert report["coupon_code"].null_count == 14
        assert report["successful"].true_count == 19

    def test_stream_utilities_with_dogfood_csv(self):
        data = INCIDENTS_CSV.read_bytes()

        schema = _run(infer_schema_stream, data, format="csv")
        count = _run(quick_row_count_stream, data, format="csv")

        assert schema.num_columns == 12
        assert "ticket_id" in schema.column_names
        assert count.count == 30

    def test_profile_csv_url(self, url_server):
        report = _run(profile_url, url_server["csv"])

        assert report.rows == 30
        assert report.columns == 12
        assert report.source_type == "stream"
        assert report["response_minutes"].max == 1440

    def test_profile_jsonl_url(self, url_server):
        report = _run(profile_url, url_server["jsonl"])

        assert report.rows == 25
        assert report.columns == 10
        assert report.source_type == "stream"
        assert report["risk_score"].max == 0.98

    def test_profile_parquet_url(self, url_server):
        try:
            report = _run(profile_url, url_server["parquet"])
        except RuntimeError as exc:
            message = str(exc)
            if "parquet-async" in message:
                assert "Remote Parquet profiling requires" in message
                return
            raise

        assert report.rows > 0
        assert report.columns > 0
        assert report.source_type == "file"
