"""Execution controls take effect, and execution metadata tells the truth (#460).

Two promises, checked across the sync and async entry points:

1. ``chunk_size`` and ``memory_limit_mb`` reach the engine that does the work.
   Both were previously accepted and dropped on the default and incremental
   paths, so a resource control could be silently ineffective.
2. ``rows``, ``bytes_consumed``, ``source_exhausted`` and ``truncation_reason``
   agree with each other. These four fields are how an agent or quality gate
   tells a complete profile from a bounded one, so they must never contradict.

Units: ``chunk_size`` is **bytes**, everywhere. Row caps are hard caps. Byte
caps are evaluated at chunk boundaries, so ``bytes_consumed`` may exceed the cap
by at most one chunk — a bound the caller sets via ``chunk_size``.

Run after building the extension:
    maturin develop --features python,python-async,async-streaming
    pytest python/tests/test_execution_controls.py -v
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import dataprof as dp
import pytest

_HAS_ASYNC = dp.capabilities().async_streaming
requires_async = pytest.mark.skipif(
    not _HAS_ASYNC,
    reason="Async streaming not compiled. Build with --features "
    "'python,python-async,async-streaming'.",
)


def _write_csv(tmp_path: Path, rows: int) -> str:
    target = tmp_path / "data.csv"
    lines = ["id,name,value,category,ts"]
    lines += [f"{i},name_{i},{i * 3},cat_{i % 7},2026-01-{(i % 28) + 1:02d}" for i in range(rows)]
    target.write_text("\n".join(lines) + "\n")
    return str(target)


def _csv_bytes(rows: int) -> bytes:
    body = "".join(f"{i},name_{i},{i * 3}\n" for i in range(rows))
    return ("id,name,value\n" + body).encode()


def _bytes_consumed(report) -> int:
    return report.to_dict()["execution"].get("bytes_consumed") or 0


def _assert_consistent(report, source_size: int, label: str) -> None:
    """The invariants every report must satisfy, whatever produced it."""
    assert report.source_exhausted == (report.truncation_reason is None), (
        f"{label}: a truncated scan is exactly a non-exhausted one "
        f"(exhausted={report.source_exhausted}, reason={report.truncation_reason})"
    )
    consumed = _bytes_consumed(report)
    if report.source_exhausted:
        assert consumed == source_size, f"{label}: an exhausted source consumed all its bytes"
    else:
        assert 0 < consumed <= source_size, f"{label}: consumed {consumed} of {source_size}"
    if report.rows > 0:
        assert consumed > 0, f"{label}: {report.rows} rows were read from 0 bytes"


def _async_bytes(data: bytes, fmt: str = "csv", **kwargs):
    from dataprof.asyncio import profile_bytes

    async def _inner():
        return await profile_bytes(data, format=fmt, **kwargs)

    return asyncio.run(_inner())


# --- Controls are not decorative --------------------------------------------


def test_chunk_size_changes_where_a_byte_cap_lands(tmp_path):
    path = _write_csv(tmp_path, 5_000)
    small = dp.profile(
        path, engine="incremental", chunk_size=4096, stop_condition=dp.StopCondition.max_bytes(2048)
    )
    large = dp.profile(
        path,
        engine="incremental",
        chunk_size=65536,
        stop_condition=dp.StopCondition.max_bytes(2048),
    )
    assert small.rows < large.rows, "a smaller chunk must stop sooner"


def test_chunk_size_reaches_the_default_engine(tmp_path):
    path = _write_csv(tmp_path, 5_000)
    small = dp.profile(path, chunk_size=4096, stop_condition=dp.StopCondition.max_bytes(2048))
    large = dp.profile(path, chunk_size=65536, stop_condition=dp.StopCondition.max_bytes(2048))
    assert small.rows < large.rows, "engine='auto' must forward chunk_size"


@pytest.mark.parametrize("chunk_size", [1024, 65536, None])
def test_chunk_size_never_changes_a_complete_profile(tmp_path, chunk_size):
    path = _write_csv(tmp_path, 2_000)
    size = Path(path).stat().st_size

    report = dp.profile(path, engine="incremental", chunk_size=chunk_size)

    assert report.rows == 2_000
    assert report.columns == 5
    _assert_consistent(report, size, f"chunk_size={chunk_size}")


@pytest.mark.parametrize("engine", ["auto", "incremental"])
def test_memory_limit_reaches_the_engine(tmp_path, engine):
    path = _write_csv(tmp_path, 20_000)
    size = Path(path).stat().st_size

    tight = dp.profile(path, engine=engine, memory_limit_mb=1)
    loose = dp.profile(path, engine=engine, memory_limit_mb=64)

    # The limit bounds retained state; it never drops rows.
    assert tight.rows == loose.rows == 20_000
    _assert_consistent(tight, size, f"{engine} tight")
    _assert_consistent(loose, size, f"{engine} loose")


# --- Provenance agrees with what happened -----------------------------------


@pytest.mark.parametrize(
    "condition",
    [
        dp.StopCondition.confidence_threshold(0.9),
        dp.StopCondition.schema_stable(50),
        dp.StopCondition.quality_sample(),
    ],
)
def test_condition_met_on_the_last_chunk_is_not_a_truncation(tmp_path, condition):
    path = _write_csv(tmp_path, 5_000)
    size = Path(path).stat().st_size

    # One chunk larger than the file: the condition becomes true on the same
    # chunk that finishes the source, so nothing was left unread.
    report = dp.profile(path, engine="incremental", chunk_size=size * 2, stop_condition=condition)

    assert report.rows == 5_000
    assert report.source_exhausted, f"complete scan reported {report.truncation_reason}"
    assert report.truncation_reason is None
    _assert_consistent(report, size, "last-chunk condition")


def test_genuine_early_stop_still_reports_truncation(tmp_path):
    path = _write_csv(tmp_path, 5_000)
    size = Path(path).stat().st_size

    report = dp.profile(
        path,
        engine="incremental",
        chunk_size=4096,
        stop_condition=dp.StopCondition.confidence_threshold(0.9),
    )

    assert report.rows < 5_000
    assert not report.source_exhausted
    assert report.truncation_reason is not None
    _assert_consistent(report, size, "early stop")


def test_schema_stable_keeps_byte_provenance(tmp_path):
    path = _write_csv(tmp_path, 5_000)
    size = Path(path).stat().st_size

    report = dp.profile(
        path,
        engine="incremental",
        chunk_size=4096,
        stop_condition=dp.StopCondition.schema_stable(50),
    )

    assert not report.source_exhausted
    assert _bytes_consumed(report) > 0, "a stopped scan must account for the bytes it read"
    _assert_consistent(report, size, "schema stable")


@pytest.mark.parametrize("limit", [1, 123, 4999])
def test_row_caps_are_hard_caps(tmp_path, limit):
    path = _write_csv(tmp_path, 5_000)
    size = Path(path).stat().st_size

    report = dp.profile(path, engine="incremental", stop_condition=dp.StopCondition.max_rows(limit))

    assert report.rows == limit, "max_rows must be exact, not approximate"
    _assert_consistent(report, size, f"max_rows({limit})")


def test_byte_cap_overshoot_is_bounded_by_one_chunk(tmp_path):
    path = _write_csv(tmp_path, 5_000)
    size = Path(path).stat().st_size

    for chunk in (4096, 16384, 65536):
        report = dp.profile(
            path,
            engine="incremental",
            chunk_size=chunk,
            stop_condition=dp.StopCondition.max_bytes(2048),
        )
        consumed = _bytes_consumed(report)
        assert consumed <= 2048 + chunk, f"chunk {chunk}: consumed {consumed} for a 2048 cap"
        _assert_consistent(report, size, f"byte cap, chunk {chunk}")


# --- Async paths ------------------------------------------------------------


@requires_async
@pytest.mark.parametrize("fmt", ["csv", "jsonl", "json"])
def test_async_row_caps_do_not_overshoot(fmt):
    if fmt == "csv":
        data = _csv_bytes(500)
    elif fmt == "jsonl":
        data = b"".join(b'{"id":%d,"v":"x%d"}\n' % (i, i) for i in range(500))
    else:
        data = b"[" + b",".join(b'{"id":%d,"v":"x%d"}' % (i, i) for i in range(500)) + b"]"

    report = _async_bytes(data, fmt, chunk_size=1024, max_rows=123)

    assert report.rows == 123, f"{fmt}: the cap must be exact, got {report.rows}"
    assert report.truncation_reason == "max_rows(123)"
    assert not report.source_exhausted


@requires_async
def test_async_row_cap_equal_to_the_row_count_is_complete():
    data = _csv_bytes(123)

    report = _async_bytes(data, max_rows=123)

    assert report.rows == 123
    assert report.source_exhausted, "the cap landed on the final row"
    assert report.truncation_reason is None
    _assert_consistent(report, len(data), "async exact-fit cap")


@requires_async
@pytest.mark.parametrize("chunk_size", [512, 65536, None])
def test_async_chunk_size_never_changes_a_complete_profile(chunk_size):
    data = _csv_bytes(2_000)

    report = _async_bytes(data, chunk_size=chunk_size)

    assert report.rows == 2_000
    assert report.columns == 3
    _assert_consistent(report, len(data), f"async chunk_size={chunk_size}")


@requires_async
@pytest.mark.parametrize(
    ("fmt", "data"),
    [
        ("json", b'[{"id":1},{"id":2}]'),
        ("jsonl", b'{"id":1}\n{"id":2}\n'),
    ],
)
def test_async_json_bytes_account_for_the_complete_buffer(fmt, data):
    report = _async_bytes(data, fmt)

    assert report.rows == 2
    _assert_consistent(report, len(data), f"async {fmt} bytes")


@requires_async
@pytest.mark.parametrize(
    ("fmt", "data"),
    [
        ("json", b'[{"id":1},{"id":2},{"id":3}]'),
        ("jsonl", b'{"id":1}\n{"id":2}\n{"id":3}\n'),
    ],
)
def test_async_json_row_cap_keeps_partial_byte_accounting(fmt, data):
    report = _async_bytes(data, fmt, max_rows=1)

    assert report.rows == 1
    assert not report.source_exhausted
    _assert_consistent(report, len(data), f"async bounded {fmt} bytes")
