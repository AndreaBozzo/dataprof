"""An async stream records the format it parsed (#1042).

The baseline gate reads a capped async CSV as declaring every column in its
header, as it reads a capped synchronous one, so a missing required column
fails instead of staying ``evidence_incomplete``. A JSONL stream still covers
only the records read.

``tests/async_stream_format.rs`` is the Rust twin; it also covers a reader
from an undeclared system and a buffer from a declared one.
"""

from __future__ import annotations

import asyncio
import http.server
import json
import socketserver
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest

try:
    import dataprof as dp
    from dataprof.asyncio import _HAS_ASYNC, _HAS_URL, profile_file, profile_url
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

pytestmark = pytest.mark.skipif(not _HAS_ASYNC, reason="async support not compiled")

#: format -> (payload without a `note` column in its first record, the
#: outcome of requiring `note` on a scan capped at one row)
CASES = {
    "csv": (b"id,amount\n1,10\n2,20\n3,30\n", "failed"),
    "jsonl": (
        b'{"id": 1}\n{"id": 2}\n{"id": 3, "note": "a"}\n',
        "evidence_incomplete:truncated",
    ),
    "json": (
        b'[{"id": 1}, {"id": 2}, {"id": 3, "note": "a"}]',
        "evidence_incomplete:truncated",
    ),
}


def _note_outcome(report: dp.ProfileReport) -> str:
    (check,) = report.check(require_columns=["note"]).checks
    if check.status != "not_evaluated":
        return check.status
    assert check.reason is not None
    return f"{check.reason['reason']}:{check.reason.get('gap')}"


class _Server(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


@pytest.fixture()
def base_url() -> Iterator[str]:
    """Serve each case's payload at ``/data.<format>``."""

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            body = CASES[self.path.rsplit(".", 1)[-1]][0]
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    with _Server(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_address[1]}"
        finally:
            server.shutdown()


def _routes(fmt: str, payload: bytes, tmp_path: Path, base_url: str):
    path = tmp_path / f"no_note.{fmt}"
    path.write_bytes(payload)
    yield "profile_file", asyncio.run(profile_file(path, max_rows=1))
    if _HAS_URL:
        url = f"{base_url}/data.{fmt}"
        yield "profile_url", asyncio.run(profile_url(url, max_rows=1))


def test_a_capped_async_csv_stream_declares_its_columns(tmp_path: Path, base_url: str) -> None:
    wrong = {}
    for fmt, (payload, expected) in CASES.items():
        for route, report in _routes(fmt, payload, tmp_path, base_url):
            label = f"{fmt} {route}"
            document = json.loads(report.to_json())
            source = document["data_source"]
            if source["type"] != "stream" or source.get("format") != fmt:
                wrong[f"{label} source"] = (source["type"], source.get("format"))
            if document["execution"]["source_exhausted"]:
                wrong[f"{label} cap"] = "the cap did not stop the scan"
            outcome = _note_outcome(report)
            if outcome != expected:
                wrong[f"{label} require note"] = outcome
            # A saved report reads back with its format.
            reloaded = dp.ProfileReport.from_dict(json.loads(report.to_json()))
            if _note_outcome(reloaded) != expected:
                wrong[f"{label} reloaded"] = _note_outcome(reloaded)
    assert not wrong, wrong


def test_a_stream_saved_without_a_format_stays_conservative(tmp_path: Path) -> None:
    path = tmp_path / "no_note.csv"
    path.write_bytes(CASES["csv"][0])
    document = json.loads(asyncio.run(profile_file(path, max_rows=1)).to_json())
    del document["data_source"]["format"]
    legacy = dp.ProfileReport.from_dict(document)
    assert _note_outcome(legacy) == "evidence_incomplete:truncated"
