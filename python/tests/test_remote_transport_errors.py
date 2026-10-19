"""Remote transport failures are catchable as OSError without hiding bad data (#946)."""

from __future__ import annotations

import asyncio
import http.server
import os
import socket
import socketserver
import struct
import threading
import time

import dataprof
import pytest
from dataprof.asyncio import profile_url

pytestmark = pytest.mark.skipif(
    not dataprof.capabilities().url_profiling, reason="URL profiling is not compiled"
)


def _profile(url, **kwargs):
    return asyncio.run(profile_url(url, **kwargs))


def _causes(error):
    while error.__cause__ is not None:
        error = error.__cause__
        yield error


@pytest.mark.parametrize("extension", ["csv", "json", "jsonl", "parquet"])
def test_closed_port_is_an_oserror_with_the_original_diagnostic(extension):
    if extension == "parquet" and not dataprof.capabilities().remote_parquet:
        pytest.skip("Remote Parquet is not compiled")
    # Keep the port bound, but never listen, so another process cannot reuse it.
    with socket.socket() as closed_port:
        closed_port.bind(("127.0.0.1", 0))
        url = f"http://127.0.0.1:{closed_port.getsockname()[1]}/data.{extension}"
        with pytest.raises(OSError) as caught:
            _profile(url)
    error = caught.value
    prefix = (
        "Analysis failed: Parquet processing failed: Failed to HEAD url: "
        if extension == "parquet"
        else "Analysis failed: Streaming processing failed: HTTP request failed: "
    )
    assert str(error).startswith(prefix)
    assert url in str(error)
    if extension != "parquet":
        assert str(error).endswith("\nCheck the URL and network connectivity")
    assert list(_causes(error)), "the original network failure must remain visible"


class _Server(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


@pytest.fixture()
def remote_server():
    corrupt = b"this is not a parquet file"

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_HEAD(self):
            if self.path.startswith("/mixed"):
                self._reset()
                return
            if self.path.startswith("/invalid-http"):
                self._invalid_http()
                return
            if self.path.startswith("/ignored"):
                self.send_response(405)
                self.send_header("Content-Length", "0")
            else:
                self.send_response(200)
                self.send_header("Content-Length", str(len(corrupt)))
            self.end_headers()

        def do_GET(self):
            if self.path.startswith("/json-record-"):
                self._json_record_response()
                return
            if self.path.startswith("/invalid-http"):
                self._invalid_http()
                return
            if self.path.startswith("/status"):
                self.send_response(503)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if self.path.startswith(("/ignored", "/mixed")):
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            ranged = self.headers.get("Range")
            start, end = 0, len(corrupt) - 1
            if ranged:
                first, _, last = ranged.removeprefix("bytes=").partition("-")
                start, end = int(first), int(last)
            chunk = corrupt[start : end + 1]
            reset = self.path.startswith("/reset")
            self.send_response(206 if ranged else 200)
            malformed_chunk = self.path.startswith("/malformed-chunk")
            if malformed_chunk:
                self.send_header("Transfer-Encoding", "chunked")
            else:
                self.send_header("Content-Length", str(len(chunk) if ranged else 1_000_000))
            if ranged:
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(corrupt)}")
            self.end_headers()
            if malformed_chunk:
                self.wfile.write(b"F")  # No CRLF after the chunk size.
                self.wfile.flush()
                self.close_connection = True
                return
            if reset:
                # Deliver valid response headers, then reset the still-incomplete
                # body. This exercises the ranged GET, not just the initial HEAD.
                self.wfile.flush()
                time.sleep(0.02)
                self._reset()
                return
            self.wfile.write(chunk)

        def _json_record_response(self):
            mode = self.path.removeprefix("/json-record-").removesuffix(".json")
            prefix = b'[{"x":1},{"x":"second'
            payload = b'[{"x":1},{"x":"second value"}]'
            if mode == "syntax":
                payload = b'[{"x":1},{"x":}]'
            elif mode == "incomplete":
                payload = prefix
            self.send_response(200)
            if mode == "malformed-chunk":
                self.send_header("Transfer-Encoding", "chunked")
            else:
                self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            if mode in {"valid", "syntax", "incomplete"}:
                self.wfile.write(payload)
                return
            if mode == "malformed-chunk":
                self.wfile.write(f"{len(prefix):X}\r\n".encode())
                self.wfile.write(prefix + b"\r\n")
            else:
                self.wfile.write(prefix)
            self.wfile.flush()
            # The failure must occur inside the second JSON value, after a
            # valid first record, rather than while reading response headers.
            time.sleep(0.02)
            if mode == "reset":
                self._reset()
            else:
                if mode == "malformed-chunk":
                    self.wfile.write(b"ZZ\r\n")
                    self.wfile.flush()
                self.close_connection = True

        def log_message(self, format, *args):
            return

        def _invalid_http(self):
            self.wfile.write(b"THIS IS NOT HTTP\r\n\r\n")
            self.wfile.flush()
            self.close_connection = True

        def _reset(self):
            linger = struct.pack("HH" if os.name == "nt" else "ii", 1, 0)
            self.connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, linger)
            self.close_connection = True
            # Close here, before TCPServer.shutdown_request can send FIN via
            # shutdown(SHUT_WR). Closing the file wrapper first releases its
            # socket reference so SO_LINGER's abortive close really sends RST.
            self.rfile.close()
            self.connection.close()

    server = _Server(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.parametrize("extension", ["csv", "json", "jsonl", "parquet"])
def test_reset_while_reading_the_body_is_an_oserror(remote_server, extension):
    if extension == "parquet" and not dataprof.capabilities().remote_parquet:
        pytest.skip("Remote Parquet is not compiled")
    kwargs = {"csv_delimiter": ","} if extension == "csv" else {}
    with pytest.raises(OSError) as caught:
        _profile(f"{remote_server}/reset.{extension}", **kwargs)
    assert list(_causes(caught.value)), "a read failure must retain its originating error"
    if extension == "parquet":
        assert str(caught.value).startswith(
            "Analysis failed: Parquet processing failed: Failed to create Parquet stream builder: "
            "Parquet error: failed to fetch byte body from http stream: "
        )


def test_http_status_keeps_its_runtime_error_and_message(remote_server):
    url = f"{remote_server}/status.csv"
    with pytest.raises(RuntimeError) as caught:
        _profile(url)
    assert str(caught.value) == (
        f"Analysis failed: Streaming processing failed: HTTP 503 Service Unavailable for {url}\n"
        "Check that the URL is public and the server is up"
    )
    assert caught.value.__cause__ is None


@pytest.mark.parametrize("extension", ["csv", "parquet"])
def test_malformed_http_response_keeps_its_runtime_error(remote_server, extension):
    if extension == "parquet" and not dataprof.capabilities().remote_parquet:
        pytest.skip("Remote Parquet is not compiled")
    with pytest.raises(RuntimeError) as caught:
        _profile(f"{remote_server}/invalid-http.{extension}")
    assert str(caught.value).startswith("Analysis failed: ")


@pytest.mark.parametrize("extension", ["csv", "parquet"])
def test_malformed_chunk_framing_keeps_its_original_error_class(remote_server, extension):
    if extension == "parquet" and not dataprof.capabilities().remote_parquet:
        pytest.skip("Remote Parquet is not compiled")
    expected = RuntimeError if extension == "parquet" else ValueError
    kwargs = {"csv_delimiter": ","} if extension == "csv" else {}
    with pytest.raises(expected, match="error decoding response body"):
        _profile(f"{remote_server}/malformed-chunk.{extension}", **kwargs)


def test_mixed_discovery_failure_uses_the_retained_head_transport_cause(remote_server):
    if not dataprof.capabilities().remote_parquet:
        pytest.skip("Remote Parquet is not compiled")
    with pytest.raises(OSError) as caught:
        _profile(f"{remote_server}/mixed.parquet")
    message = str(caught.value)
    assert message.startswith("Analysis failed: Parquet processing failed: Failed to HEAD url: ")
    assert "; range probe fallback failed: server ignored Range header" in message
    assert list(_causes(caught.value)), "the HEAD transport cause is retained by discovery"


def test_ignored_range_keeps_its_runtime_error(remote_server):
    if not dataprof.capabilities().remote_parquet:
        pytest.skip("Remote Parquet is not compiled")
    with pytest.raises(RuntimeError) as caught:
        _profile(f"{remote_server}/ignored.parquet")
    assert str(caught.value) == (
        "Analysis failed: Parquet processing failed: HTTP HEAD request returned status "
        "405 Method Not Allowed; range probe fallback failed: server ignored Range header during "
        "size probe and returned 200 OK. This server does not support HTTP Range requests, which "
        "remote Parquet profiling needs. Download the file and profile the local copy instead."
    )
    assert caught.value.__cause__ is None


def test_reachable_corrupt_parquet_keeps_its_decode_error(remote_server):
    if not dataprof.capabilities().remote_parquet:
        pytest.skip("Remote Parquet is not compiled")
    with pytest.raises(RuntimeError) as caught:
        _profile(f"{remote_server}/corrupt.parquet")
    assert str(caught.value) == (
        "Analysis failed: Parquet processing failed: Failed to create Parquet stream builder: "
        "Parquet error: Invalid Parquet file. Corrupt footer"
    )
    assert caught.value.__cause__ is not None
    assert str(caught.value.__cause__) == "Parquet error: Invalid Parquet file. Corrupt footer"


@pytest.mark.parametrize("policy", ["skip", "strict"])
def test_reset_inside_a_json_record_is_an_oserror_for_every_policy(remote_server, policy):
    with pytest.raises(OSError) as caught:
        _profile(f"{remote_server}/json-record-reset.json", jsonl_on_error=policy)

    assert str(caught.value).startswith(
        "JSON parsing failed: malformed JSON record: error decoding response body"
    )
    assert str(caught.value).endswith("\nVerify JSON format and encoding")
    assert list(_causes(caught.value)), "the JSON decoder must retain the network failure"


@pytest.mark.parametrize("policy", ["skip", "strict"])
@pytest.mark.parametrize("mode", ["truncated", "malformed-chunk"])
def test_protocol_read_errors_inside_json_are_not_skipped_or_oserrors(remote_server, policy, mode):
    # These are HTTP framing failures, not a TCP reset. Preserve their prior
    # strict-mode class while refusing to turn the failed read into a partial
    # successful report under the default skip policy.
    with pytest.raises(ValueError, match="error decoding response body"):
        _profile(f"{remote_server}/json-record-{mode}.json", jsonl_on_error=policy)


@pytest.mark.parametrize("policy", ["skip", "strict"])
@pytest.mark.parametrize("mode", ["valid", "syntax", "incomplete"])
def test_complete_http_body_preserves_json_record_policy(remote_server, policy, mode):
    # Here Content-Length matches the actual bytes, including the case where
    # the JSON document itself ends too soon. JSON EOF is not an I/O error.
    url = f"{remote_server}/json-record-{mode}.json"
    if mode != "valid" and policy == "strict":
        with pytest.raises(ValueError, match="malformed JSON record"):
            _profile(url, jsonl_on_error=policy)
        return

    report = _profile(url, jsonl_on_error=policy)
    assert report.rows == (2 if mode == "valid" else 1)
    assert report.error_count == (0 if mode == "valid" else 1)
