"""Arrow import contract: every batch, and every PyCapsule producer.

Two guards on ``import_from_pyarrow``, both invisible to value-based tests:

``profile()`` documents accepting "Arrow PyCapsule-compatible objects", but the
import path matched on the Python type name being ``Table`` or ``RecordBatch``
and refused everything else, so a conforming producer that was not pyarrow's own
type was rejected. The duck-typed branch existed elsewhere in the file and was
not reachable from this path.

Worse, a chunked ``Table`` was imported as ``to_batches()[0]``. Profiling a
multi-batch table silently reported on its first chunk only — the report looked
entirely normal, just computed over a prefix of the data. Silent divergence is
the failure mode worth the most guarding, because nothing about the output
suggests anything went wrong.
"""

from __future__ import annotations

import dataprof
import pytest

pa = pytest.importorskip("pyarrow", reason="pyarrow is required for Arrow import tests")


def _batch(values):
    return pa.record_batch({"a": pa.array(values)})


class ArrowPyCapsuleProducer:
    """A minimal Arrow PyCapsule producer that is not a pyarrow type.

    Delegates to a RecordBatch, so the exported data is identical to what
    pyarrow itself would hand over. The only variable under test is the type of
    the Python object.
    """

    def __init__(self, batch):
        self._batch = batch

    def __arrow_c_schema__(self):
        return self._batch.__arrow_c_schema__()

    def __arrow_c_array__(self, requested_schema=None):
        return self._batch.__arrow_c_array__(requested_schema)


def test_multi_batch_table_profiles_every_row():
    """A chunked Table must be profiled whole, not just its first batch."""
    batch = _batch([1, 2, 3, 4])
    table = pa.Table.from_batches([batch, batch, batch])
    assert table.num_rows == 12
    assert len(table.to_batches()) == 3

    report = dataprof.profile(table, name="multi_batch")
    assert report.rows == 12


def test_single_batch_table_is_unchanged():
    """The common single-chunk path must keep working."""
    table = pa.Table.from_batches([_batch([1, 2, 3, 4])])
    assert dataprof.profile(table, name="single_batch").rows == 4


def test_chunking_does_not_change_the_report():
    """Same rows, different chunking, same column statistics.

    Comparing row counts alone would miss a regression that reads only some
    chunks yet still reports the right total, and using identical values in
    every batch would hide one that reads only the first. Distinct values per
    batch make the statistics depend on every chunk, and the comparison is over
    the serialized column sections rather than a single number.
    """
    batches = [
        _batch([1, 2, 3, 4]),
        _batch([10, 20, 30, 40]),
        _batch([100, 200, 300, 400]),
    ]
    chunked = pa.Table.from_batches(batches)
    combined = chunked.combine_chunks()

    chunked_columns = dataprof.profile(chunked, name="chunked").to_dict()["columns"]
    combined_columns = dataprof.profile(combined, name="combined").to_dict()["columns"]
    assert chunked_columns == combined_columns

    # The result has to actually depend on the later chunks, or the equality
    # above would hold just as well for code that reads only the first one.
    first_only = pa.Table.from_batches(batches[:1])
    first_only_columns = dataprof.profile(first_only, name="first").to_dict()["columns"]
    assert chunked_columns != first_only_columns


@pytest.mark.parametrize(
    ("max_rows", "expected_rows", "expect_truncated"),
    [
        (2, 2, True),  # inside the first batch
        (4, 4, True),  # exactly a batch boundary, with batches still unread
        (6, 6, True),  # spans two batches
        (12, 12, False),  # the whole table
        (20, 12, False),  # limit above the total
    ],
)
def test_max_rows_across_chunks(max_rows, expected_rows, expect_truncated):
    """A row limit applies across the batch sequence, and reports truncation.

    The boundary case is ``max_rows=4`` on 4-row batches: the limit is reached
    exactly, with two batches still unread. Deciding truncation by comparing a
    final row count against the limit cannot see that, so the flag is decided
    while walking the sequence.
    """
    batch = _batch([1, 2, 3, 4])
    table = pa.Table.from_batches([batch, batch, batch])
    assert table.num_rows == 12

    report = dataprof.profile(table, name="limited", max_rows=max_rows)
    assert report.rows == expected_rows
    assert (report.truncation_reason is not None) is expect_truncated


def test_arrow_pycapsule_producer_is_accepted():
    """Any object implementing the PyCapsule interface is a valid source."""
    batch = _batch([1, 2, 3, 4])
    report = dataprof.profile(ArrowPyCapsuleProducer(batch), name="capsule")
    assert report.rows == 4


def test_pycapsule_producer_matches_record_batch():
    """The wrapper and the batch it wraps must profile identically."""
    batch = _batch([1, 2, 3, 4])
    assert dataprof.profile(ArrowPyCapsuleProducer(batch), name="capsule").rows == (
        dataprof.profile(batch, name="batch").rows
    )


def test_non_struct_arrow_array_is_refused_cleanly():
    """A bare Array exports a capsule but is not a record batch.

    Accepting every __arrow_c_array__ producer widens what reaches the
    import path, and a record batch is specifically a *struct* array --
    StructArray::from panics on anything else. Without an explicit check
    this input degrades from a TypeError into an "entered unreachable code"
    panic crossing the FFI boundary, which is worse than the behaviour being
    replaced. pyo3 derives PanicException from BaseException, so asserting
    TypeError here is what pins the distinction.
    """
    with pytest.raises(TypeError):
        dataprof.profile(pa.array([1, 2, 3]), name="bare_array")


def test_object_without_arrow_support_is_refused():
    """A plain object is not an Arrow source."""
    with pytest.raises(TypeError):
        dataprof.profile(object(), name="not_arrow")
