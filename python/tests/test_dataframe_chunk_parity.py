"""Chunked pandas and polars frames must be profiled whole, not first-chunk-only.

``import_batches_from_pyarrow`` was taught to walk every batch of a chunked
Table, and ``profile_arrow`` was rebuilt around that sequence. The pandas and
polars paths were not: both converted to a pyarrow Table and then imported
``to_batches()[0]``, so a frame carrying more than one Arrow chunk was profiled
over its first chunk alone.

Nothing about that output looks wrong. The row count, the null rates and the
statistics are all internally consistent; they simply describe a prefix of the
data. It is the failure mode ``AGENTS.md`` singles out as the worst for a
profiler, and it sat on the two most common inputs while the path with no
library-specific code was correct.

Chunking is not exotic on either library: ``pl.concat(..., rechunk=False)`` and
``vstack`` leave polars frames multi-chunk, and any pandas frame backed by an
Arrow ``ChunkedArray`` round-trips through ``Table.from_pandas`` as several
batches.

The row-count assertions are the cheap half. The comparisons against a
rechunked frame are the half that catches a fix which reads every batch but
folds only some of them into the statistics.
"""

from __future__ import annotations

import dataprof
import pytest

pa = pytest.importorskip("pyarrow", reason="pyarrow is required for Arrow import tests")

# Distinct values per chunk, so every statistic depends on every chunk. Repeating
# one block would let a first-chunk-only read produce the right answer by luck.
CHUNKS = [
    {"n": [1, 2, 3], "s": ["a", "b", "c"]},
    {"n": [10, 20, 30], "s": ["d", "e", "f"]},
    {"n": [100, 200, 300], "s": ["g", "h", "i"]},
]
TOTAL_ROWS = sum(len(chunk["n"]) for chunk in CHUNKS)


def _chunked_table():
    """A pyarrow Table of three batches. The reference every frame is built from."""
    return pa.Table.from_batches([pa.record_batch(chunk) for chunk in CHUNKS])


def _columns(source, name):
    return dataprof.profile(source, name=name).to_dict()["columns"]


# ---------------------------------------------------------------- polars


def _polars_frames():
    """A multi-chunk frame and its single-chunk twin, same rows in the same order."""
    pl = pytest.importorskip("polars", reason="polars is required for polars interop tests")
    chunked = pl.concat([pl.DataFrame(chunk) for chunk in CHUNKS], rechunk=False)
    assert chunked.n_chunks() > 1, "test needs a multi-chunk frame to be meaningful"
    return chunked, chunked.rechunk()


def test_multi_chunk_polars_profiles_every_row():
    chunked, _ = _polars_frames()
    assert dataprof.profile(chunked, name="polars_chunked").rows == TOTAL_ROWS


def test_polars_chunking_does_not_change_the_report():
    """Same rows, different chunking, same column statistics."""
    chunked, rechunked = _polars_frames()
    assert _columns(chunked, "chunked") == _columns(rechunked, "rechunked")


def test_polars_report_depends_on_every_chunk():
    """Guards the comparison above against a fix that reads only the first chunk.

    If both frames were profiled first-chunk-only they would still agree with
    each other. What separates a whole read from a prefix read is disagreeing
    with the first chunk alone.
    """
    pl = pytest.importorskip("polars", reason="polars is required for polars interop tests")
    chunked, _ = _polars_frames()
    first_chunk_only = pl.DataFrame(CHUNKS[0])
    assert _columns(chunked, "chunked") != _columns(first_chunk_only, "first")


def test_polars_max_rows_spans_chunks():
    """A row limit is applied across the sequence, not inside the first chunk."""
    chunked, _ = _polars_frames()
    report = dataprof.profile(chunked, name="polars_limited", max_rows=5)
    assert report.rows == 5
    assert report.truncation_reason is not None


# ---------------------------------------------------------------- pandas


def _pandas_frames():
    """An Arrow-backed frame whose columns are chunked, and its combined twin."""
    pd = pytest.importorskip("pandas", reason="pandas is required for pandas interop tests")
    table = _chunked_table()
    chunked = table.to_pandas(types_mapper=pd.ArrowDtype)
    assert len(pa.Table.from_pandas(chunked).to_batches()) > 1, (
        "test needs a frame that exports as several batches to be meaningful"
    )
    combined = table.combine_chunks().to_pandas(types_mapper=pd.ArrowDtype)
    return chunked, combined


def test_multi_chunk_pandas_profiles_every_row():
    chunked, _ = _pandas_frames()
    assert dataprof.profile(chunked, name="pandas_chunked").rows == TOTAL_ROWS


def test_pandas_chunking_does_not_change_the_report():
    chunked, combined = _pandas_frames()
    assert _columns(chunked, "chunked") == _columns(combined, "combined")


def test_pandas_report_depends_on_every_chunk():
    pd = pytest.importorskip("pandas", reason="pandas is required for pandas interop tests")
    chunked, _ = _pandas_frames()
    first_chunk_only = pa.table(CHUNKS[0]).to_pandas(types_mapper=pd.ArrowDtype)
    assert _columns(chunked, "chunked") != _columns(first_chunk_only, "first")


def test_pandas_max_rows_spans_chunks():
    chunked, _ = _pandas_frames()
    report = dataprof.profile(chunked, name="pandas_limited", max_rows=5)
    assert report.rows == 5
    assert report.truncation_reason is not None


# ---------------------------------------------------------------- across paths


def test_chunked_frames_agree_with_the_arrow_path():
    """The output contract: same data, same numbers, whichever path produced them.

    The pyarrow path already walked every batch, so it is the reference the two
    library-specific paths are measured against rather than a third opinion.
    """
    polars_chunked, _ = _polars_frames()
    pandas_chunked, _ = _pandas_frames()
    arrow_columns = _columns(_chunked_table(), "arrow")

    for frame, label in ((polars_chunked, "polars"), (pandas_chunked, "pandas")):
        columns = _columns(frame, label)
        assert len(columns) == len(arrow_columns)
        for column, expected in zip(columns, arrow_columns):
            assert column["name"] == expected["name"]
            assert column["total_count"] == expected["total_count"]
            assert column["null_count"] == expected["null_count"]
