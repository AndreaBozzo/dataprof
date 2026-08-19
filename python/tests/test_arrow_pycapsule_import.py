"""Arrow import contract: a chunked Table is profiled whole.

A guard on ``import_from_pyarrow`` that value-based tests cannot see.

A chunked ``Table`` was imported as ``to_batches()[0]``. Profiling a
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
    """Same rows, different chunking, same answer.

    Stronger than the row count alone: it pins the property rather than one
    number, so a future change that drops chunks in a different way still trips.
    """
    batch = _batch([1, 2, 3, 4])
    chunked = pa.Table.from_batches([batch, batch, batch])
    combined = chunked.combine_chunks()

    assert dataprof.profile(chunked, name="chunked").rows == (
        dataprof.profile(combined, name="combined").rows
    )
