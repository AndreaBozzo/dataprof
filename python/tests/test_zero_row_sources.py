"""A zero-row source with a declared schema is analyzed, whichever path it takes.

The file paths already profiled emptiness: a header-only CSV reports 0 rows over
its two declared columns, and an empty JSON array reports 0 rows over 0 columns.
The Arrow paths refused it. ``import_batches_from_pyarrow`` raised when
``to_batches()`` came back empty, and pyarrow returns no batches at all for a
zero-row Table, so the schema was discarded along with the rows.

That is the output contract in ``AGENTS.md`` -- "profile numbers must be
identical regardless of which engine or input path produced them" -- failing on
the sharpest possible case: one path produces a profile and another produces an
exception for the same logical input.

It also sat against the repo's ``None``/empty rule. A zero-row table with a
schema is "analyzed, nothing found", which is the empty case and not the absent
one. Refusing it made emptiness unrepresentable through the Arrow entry points,
so a caller profiling a filtered frame had to special-case the filter matching
nothing, which is exactly the case they were profiling to detect.
"""

from __future__ import annotations

import dataprof
import pytest

pa = pytest.importorskip("pyarrow", reason="pyarrow is required for Arrow import tests")


def _report(source, name):
    return dataprof.profile(source, name=name).to_dict()


def _empty_pyarrow_table():
    table = pa.table({"a": pa.array([], type=pa.int64()), "b": pa.array([], type=pa.string())})
    assert table.to_batches() == [], "test needs a Table that exports no batches to be meaningful"
    return table


# ------------------------------------------------- the refusal is gone


def test_zero_row_pyarrow_table_profiles_as_zero_rows():
    report = _report(_empty_pyarrow_table(), "arrow_empty")
    assert report["execution"]["rows_processed"] == 0
    assert [column["name"] for column in report["columns"]] == ["a", "b"]


def test_zero_row_pandas_frame_profiles_as_zero_rows():
    pd = pytest.importorskip("pandas", reason="pandas is required for pandas interop tests")
    frame = pd.DataFrame({"a": pd.Series([], dtype="int64"), "b": pd.Series([], dtype="object")})
    report = _report(frame, "pandas_empty")
    assert report["execution"]["rows_processed"] == 0
    assert [column["name"] for column in report["columns"]] == ["a", "b"]


def test_zero_row_polars_frame_profiles_as_zero_rows():
    pl = pytest.importorskip("polars", reason="polars is required for polars interop tests")
    frame = pl.DataFrame({"a": [], "b": []})
    report = _report(frame, "polars_empty")
    assert report["execution"]["rows_processed"] == 0
    assert [column["name"] for column in report["columns"]] == ["a", "b"]


def test_zero_row_record_batch_profiles_as_zero_rows():
    """The RecordBatch arm never went through the batch-list guard.

    It is here so the Table arm is measured against a path that was already
    correct, rather than against nothing.
    """
    batch = pa.record_batch({"a": pa.array([], type=pa.int64())})
    assert _report(batch, "batch_empty")["execution"]["rows_processed"] == 0


# ------------------------------------------------- against the file paths


def test_zero_row_table_agrees_with_a_header_only_csv(tmp_path):
    """The case the issue calls the sharp one: same shape, same answer.

    The CSV path infers types from values, and a header-only file has none, so
    it types both columns as text where the Arrow schema declares one of them
    int64. The comparison is therefore over the fields that emptiness decides --
    the counts and the ratios -- rather than over the whole column section. A
    declared type surviving is the point of carrying the schema, not a
    divergence to paper over.
    """
    csv = tmp_path / "header_only.csv"
    csv.write_text("a,b\n", encoding="utf-8")

    csv_report = _report(str(csv), "csv")
    arrow_report = _report(_empty_pyarrow_table(), "arrow")

    assert csv_report["execution"]["rows_processed"] == 0
    assert arrow_report["execution"]["rows_processed"] == 0
    assert csv_report["execution"]["columns_detected"] == 2
    assert arrow_report["execution"]["columns_detected"] == 2

    shared = (
        "name",
        "total_count",
        "null_count",
        "null_percentage",
        "unique_count",
        "uniqueness_ratio",
    )
    assert [{key: column[key] for key in shared} for column in arrow_report["columns"]] == [
        {key: column[key] for key in shared} for column in csv_report["columns"]
    ]


def test_zero_row_columns_report_absent_rather_than_zero_ratios():
    """Empty is "analyzed, nothing found"; a ratio over no values stays absent.

    ``null_percentage`` and ``uniqueness_ratio`` are nullable for exactly this
    case. A 0.0 here would read as "no nulls out of the rows we saw", which is
    a claim about rows that do not exist.
    """
    for column in _report(_empty_pyarrow_table(), "arrow_empty")["columns"]:
        assert column["total_count"] == 0
        assert column["null_count"] == 0
        assert column["null_percentage"] is None
        assert column["uniqueness_ratio"] is None


def test_zero_row_table_with_no_columns_profiles_as_nothing_at_all():
    """The Arrow twin of an empty JSON array: 0 rows over 0 columns."""
    report = _report(pa.table({}), "arrow_no_columns")
    assert report["execution"]["rows_processed"] == 0
    assert report["columns"] == []


# ------------------------------------------------- what still refuses


def test_a_producer_with_no_batches_and_no_schema_is_still_refused():
    """Nothing names the columns, so there is no profile to produce.

    This is the one case the fix does not turn into a report, and it is not the
    empty case: a source that cannot say what its columns are has not been
    analyzed. Keeping it an error is what stops the fix from inventing a
    zero-column profile for a source whose schema simply failed to arrive.
    """
    from dataprof.interop import profile_dataframe

    class Schemaless:
        def to_batches(self):
            return []

        def __arrow_c_array__(self, requested_schema=None):  # pragma: no cover - never called
            raise AssertionError("the batch list is consulted first")

    Schemaless.__name__ = "Table"

    with pytest.raises(ValueError, match="exposes no schema"):
        profile_dataframe(Schemaless(), "schemaless")
