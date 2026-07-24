"""JSONL malformed-record policy parity across file, bytes, and async inputs (#382).

The same malformed input must behave identically regardless of transport:
default (skip) yields a partial profile with ``execution.error_count`` set;
``jsonl_on_error="strict"`` raises a ValueError carrying line context, never a
raw ``json.JSONDecodeError`` and never the record contents.

The async cases are skipped when the extension is built without async support.

Run after building the extension:
    maturin develop --features python,python-async,async-streaming
    pytest python/tests/test_jsonl_error_policy.py -v
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import dataprof
import pytest

# Malformed record in the middle position.
MIXED = b'{"id":1,"x":10}\nnot-json\n{"id":2,"x":20}\n'
ALL_BAD = b"not-json\nalso-bad\n"
TRUNCATED = b'{"id":1}\n{"id":2'

_HAS_ASYNC = dataprof.capabilities().async_streaming
requires_async = pytest.mark.skipif(
    not _HAS_ASYNC,
    reason="Async streaming not compiled. Build with --features "
    "'python,python-async,async-streaming'.",
)


def _write(tmp_path: Path, data: bytes) -> str:
    """Write ``data`` to a JSONL file under pytest's per-test ``tmp_path``.

    Using the fixture directory means pytest cleans the files up automatically.
    """
    target = tmp_path / "data.jsonl"
    target.write_bytes(data)
    return str(target)


def _async_bytes(data: bytes, **kwargs):
    from dataprof.asyncio import profile_bytes

    async def _inner():
        return await profile_bytes(data, format="jsonl", **kwargs)

    return asyncio.run(_inner())


# --- Tolerant (default) parity ---------------------------------------------


def test_file_tolerant_reports_partial_and_error_count(tmp_path):
    path = _write(tmp_path, MIXED)
    report = dataprof.profile(path, format="jsonl")
    assert report.rows == 2
    assert report.error_count == 1


def test_bytes_tolerant_reports_partial_and_error_count():
    report = dataprof.profile(MIXED, format="jsonl")
    assert report.rows == 2
    assert report.error_count == 1


@requires_async
def test_async_bytes_tolerant_reports_partial_and_error_count():
    report = _async_bytes(MIXED)
    assert report.rows == 2
    assert report.error_count == 1


# --- Strict parity ----------------------------------------------------------


@pytest.mark.parametrize(
    "data",
    [
        b'not-json\n{"id":1}\n{"id":2}\n',  # first
        b'{"id":1}\nnot-json\n{"id":2}\n',  # middle
        b'{"id":1}\n{"id":2}\nnot-json\n',  # last
    ],
)
def test_file_strict_raises_valueerror(tmp_path, data):
    path = _write(tmp_path, data)
    with pytest.raises(ValueError) as excinfo:
        dataprof.profile(path, format="jsonl", jsonl_on_error="strict")
    # A dataprof error category, not a raw decoder exception.
    assert not isinstance(excinfo.value, json.JSONDecodeError)


def test_bytes_strict_raises_valueerror_with_line_and_no_record():
    with pytest.raises(ValueError) as excinfo:
        dataprof.profile(MIXED, format="jsonl", jsonl_on_error="strict")
    message = str(excinfo.value)
    assert not isinstance(excinfo.value, json.JSONDecodeError)
    assert "line 2" in message
    # The offending record text must never be echoed back.
    assert "not-json" not in message


@requires_async
def test_async_bytes_strict_raises_valueerror():
    with pytest.raises(ValueError) as excinfo:
        _async_bytes(MIXED, jsonl_on_error="strict")
    assert not isinstance(excinfo.value, json.JSONDecodeError)
    assert "not-json" not in str(excinfo.value)


# --- Edge cases -------------------------------------------------------------


def test_all_malformed_file_and_bytes_fail(tmp_path):
    path = _write(tmp_path, ALL_BAD)
    with pytest.raises(ValueError):
        dataprof.profile(path, format="jsonl")
    with pytest.raises(ValueError):
        dataprof.profile(ALL_BAD, format="jsonl")


@requires_async
def test_all_malformed_async_fails():
    with pytest.raises(ValueError):
        _async_bytes(ALL_BAD)


def test_blank_lines_are_not_counted():
    data = b'{"id":1}\n\n   \n{"id":2}\n'
    report = dataprof.profile(data, format="jsonl")
    assert report.rows == 2
    assert report.error_count == 0


def test_truncated_final_record_tolerant_parity(tmp_path):
    path = _write(tmp_path, TRUNCATED)
    for source in (path, TRUNCATED):
        report = dataprof.profile(source, format="jsonl")
        assert report.rows == 1
        assert report.error_count == 1


def test_truncated_final_record_strict_parity(tmp_path):
    path = _write(tmp_path, TRUNCATED)
    for source in (path, TRUNCATED):
        with pytest.raises(ValueError):
            dataprof.profile(source, format="jsonl", jsonl_on_error="strict")


@requires_async
def test_truncated_final_record_async_parity():
    report = _async_bytes(TRUNCATED)
    assert report.rows == 1
    assert report.error_count == 1

    with pytest.raises(ValueError):
        _async_bytes(TRUNCATED, jsonl_on_error="strict")


def test_invalid_policy_value_rejected():
    with pytest.raises(ValueError):
        dataprof.profile(MIXED, format="jsonl", jsonl_on_error="lenient")


def test_clean_input_has_zero_error_count():
    data = b'{"id":1}\n{"id":2}\n{"id":3}\n'
    report = dataprof.profile(data, format="jsonl")
    assert report.rows == 3
    assert report.error_count == 0


@requires_async
def test_clean_input_async_has_zero_error_count():
    data = b'{"id":1}\n{"id":2}\n{"id":3}\n'
    report = _async_bytes(data)
    assert report.rows == 3
    assert report.error_count == 0
