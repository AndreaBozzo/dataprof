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
    The comparison is over the whole column section, not a chosen few fields:
    ``AGENTS.md`` asks for identical numbers, and every field here does agree.
    """
    polars_chunked, _ = _polars_frames()
    pandas_chunked, _ = _pandas_frames()
    arrow_columns = _columns(_chunked_table(), "arrow")

    assert _columns(polars_chunked, "polars") == arrow_columns
    assert _columns(pandas_chunked, "pandas") == arrow_columns


def test_empty_frames_are_refused_the_same_way_on_every_path():
    """One error for an empty source, whichever library handed it over.

    The two library paths used to raise "DataFrame is empty" while pyarrow
    raised "Table is empty", because each had its own copy of the guard. Now
    that they share an importer they share the message, and this pins that so a
    later refactor cannot quietly fork it again.

    It pins the refusal itself as much as the wording. A zero-row frame with a
    schema is arguably analyzable, and the file paths do analyze it: a
    header-only CSV profiles as 0 rows over its declared columns rather than
    raising. That divergence is older than this test and is tracked in #664;
    changing it here would be a second behaviour change in one commit.
    """
    pd = pytest.importorskip("pandas", reason="pandas is required for pandas interop tests")
    pl = pytest.importorskip("polars", reason="polars is required for polars interop tests")

    sources = {
        "pyarrow": pa.table({"a": pa.array([], type=pa.int64())}),
        "pandas": pd.DataFrame({"a": pd.Series([], dtype="int64")}),
        "polars": pl.DataFrame({"a": []}),
    }

    messages = set()
    for label, source in sources.items():
        with pytest.raises(ValueError) as raised:
            dataprof.profile(source, name=label)
        messages.add(str(raised.value))

    assert len(messages) == 1, f"paths disagree on the empty-source error: {messages}"


class Table:
    """A non-pyarrow batch producer, named ``Table`` because the importer looks.

    ``profile_dataframe`` classifies anything that is not pandas, polars or
    pyarrow as a custom library, and that arm used to import the array capsule
    alone. A producer holding several batches was read down to the first one
    there for the same reason the two library paths were, on the one path with
    no library to name.

    Reachable through ``dataprof.interop.profile_dataframe``, which is public.
    ``dataprof.profile()`` sends capsule producers to the Arrow path instead.
    """

    def __init__(self, batches):
        self._batches = list(batches)

    def to_batches(self):
        return list(self._batches)

    def __arrow_c_schema__(self):
        return self._batches[0].__arrow_c_schema__()

    def __arrow_c_array__(self, requested_schema=None):
        return self._batches[0].__arrow_c_array__(requested_schema)


def test_custom_batch_producer_is_profiled_whole():
    """The custom arm walks the sequence like every other arm.

    ``interop.profile_dataframe`` returns the unwrapped Rust report, where the
    row count is ``rows_processed``.
    """
    from dataprof.interop import profile_dataframe

    producer = Table(pa.record_batch(chunk) for chunk in CHUNKS)
    assert profile_dataframe(producer, "custom").rows_processed == TOTAL_ROWS
