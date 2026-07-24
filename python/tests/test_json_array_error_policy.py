"""Structural JSON-array errors must never produce a clean profile.

File and async inputs stream array elements and can retain a valid prefix under
the default tolerant policy. Synchronous bytes decode the whole document and
therefore reject it. Every path must at least surface the malformed container,
and strict mode must always raise.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import dataprof
import pytest

INVALID_WITH_VALID_PREFIX = [
    (b'[{"id":1}', "missing closing bracket"),
    (b'[{"id":1},', "comma followed by EOF"),
    (b'[{"id":1} {"id":2}]', "missing comma"),
    (b'[{"id":1},,{"id":2}]', "doubled comma"),
    (b'[{"id":1},]', "trailing comma"),
    (b'[{"id":1}] trailing', "trailing content"),
    (b'[{"id":1}][{"id":2}]', "second top-level value"),
]

_HAS_ASYNC = dataprof.capabilities().async_streaming
requires_async = pytest.mark.skipif(
    not _HAS_ASYNC,
    reason="Async streaming not compiled. Build with --features "
    "'python,python-async,async-streaming'.",
)


def _write(tmp_path: Path, data: bytes) -> Path:
    target = tmp_path / "data.json"
    target.write_bytes(data)
    return target


def _async_bytes(data: bytes, **kwargs):
    from dataprof.asyncio import profile_bytes

    async def _inner():
        return await profile_bytes(data, format="json", **kwargs)

    return asyncio.run(_inner())


@pytest.mark.parametrize(("data", "case"), INVALID_WITH_VALID_PREFIX)
def test_file_tolerant_surfaces_invalid_container(tmp_path, data, case):
    report = dataprof.profile(_write(tmp_path, data), format="json")

    assert report.rows == 1, case
    assert report.error_count == 1, case


@pytest.mark.parametrize(("data", "case"), INVALID_WITH_VALID_PREFIX)
def test_file_strict_rejects_invalid_container(tmp_path, data, case):
    with pytest.raises(ValueError, match="malformed JSON array"):
        dataprof.profile(
            _write(tmp_path, data),
            format="json",
            jsonl_on_error="strict",
        )


@pytest.mark.parametrize(("data", "case"), INVALID_WITH_VALID_PREFIX)
def test_bytes_reject_invalid_container(data, case):
    with pytest.raises(ValueError, match="malformed JSON"):
        dataprof.profile(data, format="json")


@requires_async
@pytest.mark.parametrize(("data", "case"), INVALID_WITH_VALID_PREFIX)
def test_async_tolerant_surfaces_invalid_container(data, case):
    report = _async_bytes(data)

    assert report.rows == 1, case
    assert report.error_count == 1, case


@requires_async
@pytest.mark.parametrize(("data", "case"), INVALID_WITH_VALID_PREFIX)
def test_async_strict_rejects_invalid_container(data, case):
    with pytest.raises(ValueError, match="malformed JSON array"):
        _async_bytes(data, jsonl_on_error="strict")


@pytest.mark.parametrize("source_kind", ["file", "bytes"])
def test_invalid_before_first_value_fails(tmp_path, source_kind):
    data = b'[,{"id":1}]'
    source = _write(tmp_path, data) if source_kind == "file" else data

    with pytest.raises(ValueError):
        dataprof.profile(source, format="json")


@requires_async
def test_invalid_before_first_value_fails_async():
    with pytest.raises(ValueError):
        _async_bytes(b'[,{"id":1}]')


def test_file_max_rows_does_not_validate_unread_suffix(tmp_path):
    report = dataprof.profile(
        _write(tmp_path, b'[{"id":1},not-valid-json'),
        format="json",
        max_rows=1,
    )

    assert report.rows == 1
    assert report.error_count == 0
    assert not report.source_exhausted
