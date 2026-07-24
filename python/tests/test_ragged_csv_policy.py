"""Ragged CSV policy parity across file, bytes, and async inputs (#418, #462).

A row whose field count differs from the header is a structural violation. The
shipped policy per transport is deliberately not uniform, and this file is the
record of which difference is intended:

===================  ==========================================================
file (auto/columnar) recover, count in ``execution.ragged_row_count``
async bytes / URL    recover, count — same signal as the file path
sync bytes           reject with a ValueError naming the row (no flexible
                     engine behind the pure-Python bytes reader)
``csv_flexible``     ``False`` rejects on file *and* async paths
===================  ==========================================================

What no path may do is recover silently: a repaired scan must never report as a
clean one. Every rejection is a ``ValueError`` — malformed data is bad input,
the same category as malformed JSON — and carries the field counts rather than
being buried under "all engines failed". The async cases are skipped when the
extension is built without async support.

Run after building the extension:
    maturin develop --features python,python-async,async-streaming
    pytest python/tests/test_ragged_csv_policy.py -v
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import dataprof as dp
import pytest

# Row 2 is short (2 fields), row 3 is over-long (4 fields); both differ from the
# 3-column header, so both count.
RAGGED = b"name,age,city\nAlice,25,NYC\nBob,30\nCarol,35,LA,EXTRA\nDave,40,SF\n"
CLEAN = b"name,age,city\nAlice,25,NYC\nBob,30,LA\n"

_HAS_ASYNC = dp.capabilities().async_streaming
requires_async = pytest.mark.skipif(
    not _HAS_ASYNC,
    reason="Async streaming not compiled. Build with --features "
    "'python,python-async,async-streaming'.",
)


def _write(tmp_path: Path, data: bytes) -> str:
    target = tmp_path / "data.csv"
    target.write_bytes(data)
    return str(target)


def _async_bytes(data: bytes, **kwargs):
    from dataprof.asyncio import profile_bytes

    async def _inner():
        return await profile_bytes(data, format="csv", **kwargs)

    return asyncio.run(_inner())


# --- Recovering paths count what they repaired ------------------------------


def test_file_recovers_and_counts(tmp_path):
    report = dp.profile(_write(tmp_path, RAGGED))
    assert report.rows == 4
    assert report.ragged_row_count == 2


@requires_async
def test_async_bytes_recover_and_count():
    report = _async_bytes(RAGGED)
    assert report.rows == 4
    assert report.ragged_row_count == 2
    assert report.to_dict()["execution"]["ragged_row_count"] == 2


@requires_async
def test_async_bytes_match_the_file_path(tmp_path):
    from_file = dp.profile(_write(tmp_path, RAGGED))
    from_async = _async_bytes(RAGGED)
    assert from_async.rows == from_file.rows
    assert from_async.ragged_row_count == from_file.ragged_row_count


# --- Clean input stays clean ------------------------------------------------


def test_file_clean_reports_zero(tmp_path):
    assert dp.profile(_write(tmp_path, CLEAN)).ragged_row_count == 0


def test_bytes_clean_reports_zero():
    assert dp.profile(CLEAN, format="csv").ragged_row_count == 0


@requires_async
def test_async_bytes_clean_reports_zero():
    report = _async_bytes(CLEAN)
    assert report.rows == 2
    assert report.ragged_row_count == 0
    assert report.error_count == 0


# --- Rejecting paths --------------------------------------------------------


def test_sync_bytes_reject_and_point_at_the_flexible_engine():
    with pytest.raises(ValueError) as excinfo:
        dp.profile(RAGGED, format="csv")
    message = str(excinfo.value)
    assert "row 3" in message, message
    assert "csv_flexible" in message, message


def test_file_csv_flexible_false_rejects(tmp_path):
    with pytest.raises(ValueError) as excinfo:
        dp.profile(_write(tmp_path, RAGGED), csv_flexible=False)
    message = str(excinfo.value)
    assert "3 fields" in message, message
    # Strict parsing opts out of recovery, so the auto engine must not retry
    # under a second parser and lose the diagnostic.
    assert "All engines failed" not in message, message


@requires_async
def test_async_bytes_csv_flexible_false_rejects():
    # Previously accepted and ignored on this path: a caller asking for strict
    # parsing got a repaired report instead of an error.
    with pytest.raises(ValueError) as excinfo:
        _async_bytes(RAGGED, csv_flexible=False)
    assert "csv_flexible" in str(excinfo.value)


# --- Delimiter is a property of the data, not of the transport --------------


@requires_async
def test_async_detects_the_same_delimiter_as_the_file_path(tmp_path):
    # The async reader used to assume a comma, collapsing a semicolon source
    # into one column that profiled as perfectly clean.
    semicolon = b"name;age;city\nAlice;25;NYC\nBob;30;LA\n"
    from_file = dp.profile(_write(tmp_path, semicolon))
    from_async = _async_bytes(semicolon)

    assert from_async.columns == from_file.columns == 3
    assert from_async.rows == from_file.rows == 2


@requires_async
def test_async_honors_explicit_delimiter():
    report = _async_bytes(b"a|b|c\n1|2|3\n", csv_delimiter="|")
    assert report.columns == 3
    assert report.rows == 1
