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
    """A buffer has no path, so it reports the in-memory shape, not a fake one."""
    data = _write(tmp_path, pa.table({"id": [1, 2]})).read_bytes()
    report = dp.profile(data, format="parquet")
    assert report.source_type == "dataframe"
    assert "parquet_bytes" in report.source
    assert "pandas" not in report.source


def test_name_overrides_the_default_label(tmp_path):
    data = _write(tmp_path, pa.table({"id": [1, 2]})).read_bytes()
    assert "orders" in dp.profile(data, format="parquet", name="orders").source
