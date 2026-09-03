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


def test_zero_row_columns_carry_no_statistics_at_all():
    """Every aggregate over zero values is undefined, whatever the column's type.

    Text and date columns already reported absent statistics at zero rows.
    Numeric and boolean ones did not: an ``int64`` column came back with
    ``min``, ``max``, ``mean``, ``std_dev`` and ``variance`` of 0.0, and a
    boolean column with a ``true_ratio`` of 0.0. Those are plausible numbers
    describing rows that do not exist, which is the failure mode ``AGENTS.md``
    calls the worst for a profiler, and 0.0 is a perfectly ordinary value for a
    real numeric column, so nothing about the output looked wrong.

    The counts are deliberately not part of this. "0 of 0 values were invalid"
    is a fact about an analyzed column, not a statistic over nothing.
    """
    table = pa.table(
        {
            "i": pa.array([], type=pa.int64()),
            "f": pa.array([], type=pa.float64()),
            "b": pa.array([], type=pa.bool_()),
            "s": pa.array([], type=pa.string()),
            "d": pa.array([], type=pa.date32()),
        }
    )

    for column in _report(table, "arrow_typed_empty")["columns"]:
        assert column.get("stats") is None, (
            f"column {column['name']!r} reported statistics over zero values: {column['stats']}"
        )


def test_a_single_row_still_reports_its_statistics():
    """The half a fix like the one above gets wrong.

    Suppressing statistics when no values were analyzed must not suppress them
    for a column that has exactly one, and a genuine min of 0.0 must survive
    rather than be read back as the absent case.
    """
    table = pa.table({"i": pa.array([0], type=pa.int64()), "b": pa.array([False])})
    columns = {column["name"]: column for column in _report(table, "one_row")["columns"]}

    assert columns["i"]["stats"]["min"] == 0.0
    assert columns["i"]["stats"]["max"] == 0.0
    assert columns["b"]["stats"]["true_ratio"] == 0.0
    assert columns["b"]["stats"]["false_count"] == 1


def test_zero_row_table_with_no_columns_profiles_as_nothing_at_all():
    """The Arrow twin of an empty JSON array: 0 rows over 0 columns."""
    report = _report(pa.table({}), "arrow_no_columns")
    assert report["execution"]["rows_processed"] == 0
    assert report["columns"] == []


# ------------------------------------------------- what still refuses


def test_a_producer_carrying_the_schema_itself_keeps_its_columns():
    """The other half of the schema lookup, which pyarrow never exercises.

    pyarrow puts ``__arrow_c_schema__`` on ``Table.schema`` and not on the
    Table, so every test above takes the ``obj.schema`` branch. An object
    written against the PyCapsule interface alone carries the capsule directly,
    and that branch has no other coverage: the existing custom producer in
    ``test_dataframe_chunk_parity.py`` always has batches and never reaches the
    schema lookup at all.
    """
    from dataprof.interop import profile_dataframe

    class Table:
        """Named ``Table`` because the importer matches on the type name.

        ``__arrow_c_array__`` is declared because the custom arm of
        ``convert_dataframe_to_batches`` gates on it before importing. It is
        never called: with no batches there is no array to hand over, and the
        schema is what carries the columns.
        """

        def __init__(self, schema):
            self._schema = schema

        def to_batches(self):
            return []

        def __arrow_c_schema__(self):
            return self._schema.__arrow_c_schema__()

        def __arrow_c_array__(self, requested_schema=None):  # pragma: no cover - never called
            raise AssertionError("an empty producer has no array to export")

    producer = Table(pa.schema([("a", pa.int64()), ("b", pa.string())]))
    report = profile_dataframe(producer, "direct_schema")

    assert report.rows_processed == 0
    assert [column.name for column in report.column_profiles] == ["a", "b"]


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
