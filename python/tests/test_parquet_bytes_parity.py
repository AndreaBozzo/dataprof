"""Parquet byte buffers profile natively, identically to the file path (#461).

``dp.profile(bytes, format="parquet")`` is advertised alongside CSV/JSON/JSONL
bytes, but it used to round-trip through ``pandas.read_parquet()`` and therefore
failed in the dependency-free published wheel — even though the Rust Parquet
reader is compiled in and ``capabilities().local_parquet`` is true.

It now reads through the same Arrow/Parquet stack the file path uses, so the two
transports agree on every number by construction. These tests pin that parity
over the shapes whose typing differs between readers; ``pyarrow`` is used only
to *write* the fixtures, never to profile them.

The bare-wheel side of the contract — that no optional dependency is imported —
is asserted in ``.github/scripts/wheel_smoke.py``, which runs against an
installed wheel in a venv with nothing else in it.
"""

from __future__ import annotations

import io
from pathlib import Path

import dataprof as dp
import pytest

pa = pytest.importorskip("pyarrow", reason="pyarrow writes the fixtures")
pq = pytest.importorskip("pyarrow.parquet", reason="pyarrow writes the fixtures")


def _table_cases() -> list[tuple[str, object]]:
    """Tables covering the shapes a reader can disagree about."""
    return [
        (
            "nullable numeric",
            pa.table(
                {
                    "i32": pa.array([1, None, 3], type=pa.int32()),
                    "i64": pa.array([10, 20, None], type=pa.int64()),
                    "f64": pa.array([1.5, None, 3.5], type=pa.float64()),
                }
            ),
        ),
        (
            "boolean",
            pa.table({"flag": pa.array([True, False, None])}),
        ),
        (
            "date and timestamp",
            pa.table(
                {
                    "day": pa.array([19000, None, 19002], type=pa.date32()),
                    "moment": pa.array(
                        [1_700_000_000_000, None, 1_700_000_100_000], type=pa.timestamp("ms")
                    ),
                }
            ),
        ),
        (
            "non-finite floats",
            pa.table({"ratio": pa.array([1.5, float("nan"), float("inf"), float("-inf")])}),
        ),
        (
            "all null column",
            pa.table({"empty": pa.array([None, None, None], type=pa.string())}),
        ),
        (
            "empty schema",
            pa.table({}),
        ),
        (
            "zero rows with a schema",
            pa.table({"id": pa.array([], type=pa.int32())}),
        ),
    ]


CASES = _table_cases()
IDS = [name for name, _ in CASES]


def _write(tmp_path: Path, table, name: str = "data.parquet") -> Path:
    target = tmp_path / name
    pq.write_table(table, target)
    return target


@pytest.mark.parametrize(("label", "table"), CASES, ids=IDS)
def test_bytes_and_file_agree_on_every_column(tmp_path, label, table):
    path = _write(tmp_path, table)
    data = path.read_bytes()

    from_file = dp.profile(str(path))
    from_bytes = dp.profile(data, format="parquet")

    assert from_bytes.rows == from_file.rows, label
    assert list(from_bytes) == list(from_file), label
    assert from_bytes.to_dict()["columns"] == from_file.to_dict()["columns"], label
    assert from_bytes.quality_score == from_file.quality_score, label


def test_bytesio_matches_bytes(tmp_path):
    data = _write(tmp_path, CASES[0][1]).read_bytes()
    assert (
        dp.profile(io.BytesIO(data), format="parquet").to_dict()["columns"]
        == dp.profile(data, format="parquet").to_dict()["columns"]
    )


def test_column_order_follows_the_parquet_schema(tmp_path):
    """Not alphabetical, and not whatever a DataFrame round-trip would produce."""
    table = pa.table({"zeta": [1, 2], "alpha": [3, 4], "mid": [5, 6]})
    data = _write(tmp_path, table).read_bytes()
    assert list(dp.profile(data, format="parquet")) == ["zeta", "alpha", "mid"]


def _raised(call) -> Exception:
    """Return the exception ``call`` raised, failing if it raised nothing.

    Comparing two ``None``s would let a parity assertion pass while neither
    path rejected anything, so the rejection itself is asserted first.
    """
    try:
        call()
    except Exception as exc:  # noqa: BLE001 - which type it is, is the assertion
        return exc
    raise AssertionError("expected the call to raise, but it returned a report")


def test_duplicate_column_names_are_rejected(tmp_path):
    """Parquet permits repeated field names; a profile keyed by name cannot."""
    table = pa.Table.from_arrays(
        [pa.array([1, 2]), pa.array([3, 4])],
        schema=pa.schema([pa.field("dup", pa.int64()), pa.field("dup", pa.int64())]),
    )
    path = _write(tmp_path, table, "dup.parquet")
    data = path.read_bytes()

    file_error = _raised(lambda: dp.profile(str(path)))
    bytes_error = _raised(lambda: dp.profile(data, format="parquet"))

    assert "Duplicate column name" in str(file_error)
    assert "Duplicate column name" in str(bytes_error)
    assert type(file_error) is type(bytes_error), (
        f"file raised {file_error!r} but bytes raised {bytes_error!r}"
    )


def test_max_rows_caps_the_buffer(tmp_path):
    table = pa.table({"id": list(range(10))})
    data = _write(tmp_path, table).read_bytes()

    report = dp.profile(data, format="parquet", max_rows=4)
    assert report.rows == 4
    assert report.truncation_reason is not None


@pytest.mark.parametrize("row_group_size", [7, 25, 100])
def test_capped_sample_spans_file_and_survives_serialization(tmp_path, row_group_size):
    path = tmp_path / "spread.parquet"
    pq.write_table(pa.table({"id": list(range(100))}), path, row_group_size=row_group_size)
    native = dp.profile(path, max_rows=4)
    buffered = dp.profile(path.read_bytes(), format="parquet", max_rows=4)
    saved = tmp_path / "report.json"
    native.save(saved)
    reports = [native, buffered, dp.ProfileReport.load(saved)]
    for report in reports:
        assert report.rows == 4
        assert report["id"].min == 0
        assert report["id"].max == 99
        assert report["id"].mean == 49.5
        assert report.sampling_applied
        assert report.sampling_ratio == 0.04
        assert not report.source_exhausted
        assert report.truncation_reason == "max_rows(4)"
        assert report.sampled_row_ranges == [[0, 1], [33, 34], [66, 67], [99, 100]]
        document = report.to_dict()
        assert document["execution"]["sampled_row_ranges"] == report.sampled_row_ranges
        assert document["columns"] == native.to_dict()["columns"]
        assert document["quality"] == native.to_dict()["quality"]


@pytest.mark.parametrize("cap", [0, 1, 2, 31, 32, 33, 99, 100, 101])
def test_capped_sample_population_and_boundaries(tmp_path, cap):
    path = _write(tmp_path, pa.table({"id": list(range(100))}))
    report = dp.profile(path, max_rows=cap)
    assert report.rows == min(cap, 100)
    if cap >= 100:
        assert report.sampled_row_ranges is None
        assert "sampled_row_ranges" not in report.to_dict()["execution"]
        assert not report.sampling_applied
        assert report.source_exhausted
        return
    ranges = report.sampled_row_ranges
    assert ranges is not None
    assert len(ranges) <= 32
    selected = [i for start, end in ranges for i in range(start, end)]
    assert len(selected) == cap
    assert selected == sorted(set(selected))
    if cap == 0:
        assert ranges == []
        assert dp.ProfileReport.from_json(report.to_json()).sampled_row_ranges == []
        assert report["id"].mean is None
    elif cap == 1:
        assert selected == [50]
        assert report["id"].min == report["id"].max == 50
    else:
        assert selected[0] == 0 and selected[-1] == 99
    if selected:
        assert report["id"].mean == pytest.approx(
            sum(selected) / len(selected), rel=1e-9, abs=1e-12
        )


def test_spread_sample_counts_duplicates_across_distant_rows(tmp_path):
    data = _write(tmp_path, pa.table({"id": [7, 1, 2, 3, 7]})).read_bytes()
    report = dp.profile(data, format="parquet", max_rows=2)
    assert report.sampled_row_ranges == [[0, 1], [4, 5]]
    assert report.quality is not None
    uniqueness = report.quality.uniqueness
    assert uniqueness is not None
    assert uniqueness["duplicate_rows"] == 1


def test_legacy_prefix_report_does_not_gain_spread_provenance(tmp_path):
    data = _write(tmp_path, pa.table({"id": [1, 2, 3]})).read_bytes()
    document = dp.profile(data, format="parquet", max_rows=2).to_dict()
    del document["execution"]["sampled_row_ranges"]
    document["execution"]["sampling_applied"] = False
    document["execution"]["sampling_ratio"] = None
    restored = dp.ProfileReport.from_dict(document)
    assert restored.sampled_row_ranges is None
    assert restored.truncation_reason == "max_rows(2)"
    assert "sampled_row_ranges" not in restored.to_dict()["execution"]


def test_max_rows_equal_to_the_row_count_is_not_truncation(tmp_path):
    data = _write(tmp_path, pa.table({"id": [1, 2, 3]})).read_bytes()
    report = dp.profile(data, format="parquet", max_rows=3)
    assert report.rows == 3
    assert report.truncation_reason is None


def test_semantic_hints_reach_the_bytes_path(tmp_path):
    data = _write(tmp_path, pa.table({"amount": [1.0, 2.0, 3.0]})).read_bytes()
    report = dp.profile(data, format="parquet", positive_columns=["amount"])
    assert report.rows == 3


def test_a_buffer_that_is_not_parquet_fails_like_the_file_path(tmp_path):
    path = tmp_path / "bad.parquet"
    path.write_bytes(b"definitely not parquet")

    file_error = _raised(lambda: dp.profile(str(path)))
    bytes_error = _raised(lambda: dp.profile(b"definitely not parquet", format="parquet"))

    assert type(file_error) is type(bytes_error)


def test_bytes_report_is_labelled_as_an_in_memory_source(tmp_path):
    """A buffer has no path, so it reports the byte-buffer shape, not a fake one."""
    data = _write(tmp_path, pa.table({"id": [1, 2]})).read_bytes()
    report = dp.profile(data, format="parquet")
    assert report.source_type == "bytes"
    assert "parquet_bytes" in report.source
    assert "pandas" not in report.source


@pytest.mark.parametrize(
    ("payload", "fmt"),
    [
        (b"id,name\n1,alice\n2,bob\n", "csv"),
        (b'{"id": [1, 2], "name": ["alice", "bob"]}', "json"),
        (b'{"id": 1}\n{"id": 2}\n', "jsonl"),
    ],
)
def test_every_bytes_format_reports_the_bytes_source_type(payload, fmt):
    """`source_type` changed for all four byte inputs, not just Parquet, and
    each takes a different route into the report — the text formats decode in
    Python and go through `profile_columns`, Parquet goes through the Rust
    reader. Cover the three that share the first route."""
    report = dp.profile(payload, format=fmt)

    assert report.source_type == "bytes"
    assert f"{fmt}_bytes" in report.source


def test_a_real_dataframe_still_reports_dataframe():
    """The control for the case above: `bytes` must not have swallowed the
    dataframe label on the path that shares the same entry point."""
    report = dp.profile({"id": ["1", "2"], "name": ["alice", "bob"]})

    assert report.source_type == "dataframe"


def test_a_bytes_source_cannot_be_built_without_the_buffer_length():
    """`size_bytes` is a statement about the input handed over, and the decoded
    cells are not it — quoting, delimiters and encoding all move the number, and
    Parquet is off by the compression ratio. The entry point refuses to guess
    rather than record a plausible wrong size."""
    from dataprof._dataprof import profile_columns

    with pytest.raises(ValueError, match="source_bytes"):
        profile_columns(
            [("id", ["1", "2"])],
            "csv_bytes",
            None,
            None,
            0,
            None,
            "bytes",
            "csv",
            None,
        )


def test_name_overrides_the_default_label(tmp_path):
    data = _write(tmp_path, pa.table({"id": [1, 2]})).read_bytes()
    assert "orders" in dp.profile(data, format="parquet", name="orders").source


def test_async_profile_bytes_matches_sync(tmp_path):
    """dp.asyncio.profile_bytes routes a materialized Parquet buffer through
    the blocking reader instead of refusing it (gh #551)."""
    import asyncio

    from dataprof.asyncio import profile_bytes

    table = pa.table({"id": [1, 2, 3], "name": ["a", "b", None]})
    data = _write(tmp_path, table).read_bytes()

    sync = dp.profile(data, format="parquet")
    async_report = asyncio.run(profile_bytes(data, format="parquet"))

    assert async_report.rows == sync.rows
    assert list(async_report) == list(sync)
    assert async_report.to_dict()["columns"] == sync.to_dict()["columns"]
