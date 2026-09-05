"""Execution controls take effect, and execution metadata tells the truth (#460).

Two promises, checked across the sync and async entry points:

1. ``chunk_size`` and ``memory_limit_mb`` reach the engine that does the work.
   Both were previously accepted and dropped on the default and incremental
   paths, so a resource control could be silently ineffective.
2. ``rows``, ``bytes_consumed``, ``source_exhausted`` and ``truncation_reason``
   agree with each other. These four fields are how an agent or quality gate
   tells a complete profile from a bounded one, so they must never contradict.

Units: ``chunk_size`` is a **byte target**, everywhere. Row caps are hard caps.
Byte caps are evaluated at chunk boundaries, so ``bytes_consumed`` may exceed
the cap by one chunk, and a parser may extend that chunk to keep one logical
record intact.

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


def test_chunk_smaller_than_csv_header_keeps_the_schema_and_rows(tmp_path):
    path = tmp_path / "small_chunks.csv"
    path.write_bytes(b"alpha,beta\n1,2\n3,4\n")

    report = dp.profile(str(path), engine="incremental", chunk_size=5)

    assert report.rows == 2
    assert list(report) == ["alpha", "beta"]
    assert report.ragged_row_count == 0
    _assert_consistent(report, path.stat().st_size, "header exceeds chunk target")


@pytest.mark.parametrize("engine", ["auto", "incremental"])
def test_multiline_record_crossing_chunk_boundary_stays_one_row(tmp_path, engine):
    path = tmp_path / "multiline.csv"
    long_value = "x" * 65_490
    path.write_text(f'id,bio\n1,"{long_value}\ncontinued"\n2,plain\n', encoding="utf-8")

    report = dp.profile(str(path), engine=engine)

    assert report.rows == 2
    assert list(report) == ["id", "bio"]
    assert report.ragged_row_count == 0
    _assert_consistent(report, path.stat().st_size, f"{engine} multiline record")


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
    report = _async_bytes(data, fmt, chunk_size=8, max_rows=1)

    assert report.rows == 1
    assert not report.source_exhausted
    assert 0 < _bytes_consumed(report) < len(data)
    _assert_consistent(report, len(data), f"async bounded {fmt} bytes")


@requires_async
@pytest.mark.parametrize("engine", ["columnar", "COLUMNAR", "arrow", "ARROW"])
def test_async_bytes_reject_columnar_engine(engine):
    with pytest.raises(ValueError, match="columnar"):
        _async_bytes(_csv_bytes(3), engine=engine)


@requires_async
def test_async_parquet_honors_row_cap(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from dataprof.asyncio import profile_file

    path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"id": list(range(10))}), path)

    async def _inner():
        return await profile_file(path, max_rows=3)

    report = asyncio.run(_inner())
    assert report.rows == 3
    assert report.truncation_reason == "max_rows(3)"
    assert not report.source_exhausted


@requires_async
def test_async_parquet_rejects_unsupported_stop_condition(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from dataprof.asyncio import profile_file

    path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"id": list(range(10))}), path)

    async def _inner():
        return await profile_file(path, stop_condition=dp.StopCondition.max_bytes(1))

    with pytest.raises(ValueError, match="only row-limit"):
        asyncio.run(_inner())


# ---------------------------------------------------------------------------
# A cap of zero rows (#541)
# ---------------------------------------------------------------------------
#
# ``max_rows=0`` is the boundary where the two promises above meet: it must read
# no rows, and it must say the source was cut short. Both halves were broken,
# differently per path. The CSV and async readers checked the cap *after*
# processing a row, so a cap of zero returned one row — every positive cap was
# exact, which is why this hid. The JSON file path counted the truncation
# correctly and then dropped it, returning a report indistinguishable from a
# complete profile of an empty source.


def _json_fixtures(tmp_path: Path) -> dict[str, tuple[Path, bytes, str]]:
    """The same two records in each shape, plus the single-object document."""
    shapes = {
        "csv": ("data.csv", b"x\n1\n2\n", "csv"),
        "jsonl": ("data.jsonl", b'{"x":1}\n{"x":2}\n', "jsonl"),
        "json array": ("array.json", b'[{"x":1},{"x":2}]', "json"),
        # One record, so "nothing was read" is unambiguous rather than partial.
        "json object": ("object.json", b'{"x":1}', "json"),
    }
    out = {}
    for label, (name, payload, fmt) in shapes.items():
        path = tmp_path / name
        path.write_bytes(payload)
        out[label] = (path, payload, fmt)
    return out


@pytest.mark.parametrize("label", ["csv", "jsonl", "json array", "json object"])
def test_max_rows_zero_reads_nothing_and_says_so(label, tmp_path):
    path, payload, fmt = _json_fixtures(tmp_path)[label]

    for transport, report in [
        ("file", dp.profile(path, max_rows=0)),
        ("bytes", dp.profile(payload, format=fmt, max_rows=0)),
    ]:
        assert report.rows == 0, f"{label} {transport}: a cap of zero is a hard cap"
        assert report.truncation_reason == "max_rows(0)", (
            f"{label} {transport}: a scan cut short before the first record must say so, "
            f"got {report.truncation_reason!r}"
        )
        assert not report.source_exhausted, f"{label} {transport}: records remained unread"


def test_max_rows_zero_on_parquet_reads_nothing_and_says_so(tmp_path):
    pa = pytest.importorskip("pyarrow", reason="pyarrow writes the fixture")
    pq = pytest.importorskip("pyarrow.parquet", reason="pyarrow writes the fixture")

    path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"x": [1, 2]}), path)

    report = dp.profile(path, max_rows=0)
    assert report.rows == 0
    assert report.truncation_reason == "max_rows(0)"


@requires_async
@pytest.mark.parametrize("label", ["csv", "jsonl", "json array", "json object"])
def test_max_rows_zero_on_async_reads_nothing_and_says_so(label, tmp_path):
    path, _payload, _fmt = _json_fixtures(tmp_path)[label]

    from dataprof.asyncio import profile_file

    report = asyncio.run(profile_file(path, max_rows=0))
    assert report.rows == 0, f"{label}: a cap of zero is a hard cap"
    assert report.truncation_reason == "max_rows(0)", f"{label}: got {report.truncation_reason!r}"


@pytest.mark.parametrize(
    ("label", "name", "payload"),
    [
        ("csv header only", "empty.csv", b"x\n"),
        ("jsonl no records", "empty.jsonl", b""),
        ("json empty array", "empty.json", b"[]"),
    ],
)
def test_an_empty_source_is_not_a_truncated_one(label, name, payload, tmp_path):
    # The counterpart that makes the assertion above meaningful: reporting
    # truncation unconditionally would satisfy those tests and lie here.
    path = tmp_path / name
    path.write_bytes(payload)

    report = dp.profile(path, max_rows=0)
    assert report.rows == 0
    assert report.truncation_reason is None, (
        f"{label}: nothing was left unread, so nothing was truncated"
    )


@pytest.mark.parametrize("engine", ["auto", "incremental"])
def test_max_rows_zero_still_reports_the_bytes_it_read(engine, tmp_path):
    # Stopping before the first row does not mean nothing was read: a chunk came
    # off disk to discover the cap was already met. Reporting zero bytes here
    # would contradict `source_exhausted=False` — a scan that stopped early
    # having read nothing at all — which is exactly the contradiction
    # `_assert_consistent` exists to catch.
    rows = 500
    path = Path(_write_csv(tmp_path, rows))
    size = path.stat().st_size

    report = dp.profile(path, engine=engine, max_rows=0)

    assert report.rows == 0
    _assert_consistent(report, size, f"{engine} max_rows=0")


@pytest.mark.parametrize("cap", [1, 2, 5, 9])
def test_positive_caps_are_unchanged(cap, tmp_path):
    # The zero fix must not shift any other cap by one in either direction.
    rows = 5
    csv_path = Path(_write_csv(tmp_path, rows))
    jsonl_path = tmp_path / "data.jsonl"
    jsonl_path.write_bytes(b"".join(b'{"x":%d}\n' % i for i in range(rows)))

    expected = min(cap, rows)
    assert dp.profile(csv_path, max_rows=cap).rows == expected
    assert dp.profile(jsonl_path, max_rows=cap).rows == expected
    if _HAS_ASYNC:
        from dataprof.asyncio import profile_file

        assert asyncio.run(profile_file(csv_path, max_rows=cap)).rows == expected
