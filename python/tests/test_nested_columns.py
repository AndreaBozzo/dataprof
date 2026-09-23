"""A struct, list or map column profiles the same from every input (#637).

Such a column used to be profiled as a string rendering of its values: Arrow's
display string on the Parquet, pyarrow, pandas and polars paths, compact JSON on
the JSON and ad-hoc paths. Lengths, distinct counts and patterns therefore
depended on the format the data arrived in. A container column now reports its
counts and nothing measured on a serialisation, identically on every path.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import dataprof as dp
import dataprof.asyncio as dp_async
import pytest

pa = pytest.importorskip("pyarrow", reason="pyarrow builds the typed fixtures")
pq = pytest.importorskip("pyarrow.parquet", reason="pyarrow writes the Parquet fixture")

#: Row 2 holds null containers; row 3 a struct with a null child and an empty
#: list, where the old renderings disagreed most.
#: ``lat`` puts a ``.`` and a ``,`` into each struct's JSON text, which the
#: format check counts as a violation if container values reach it.
RECORDS = [
    {"id": 1, "address": {"city": "Rome", "zip": 100, "lat": 41.9}, "tags": ["a", "b"]},
    {"id": 2, "address": None, "tags": None},
    {"id": 3, "address": {"city": None, "zip": 200, "lat": 45.4}, "tags": []},
    {"id": 4, "address": {"city": "Milan", "zip": 300, "lat": 45.5}, "tags": ["c"]},
]


def _table() -> pa.Table:
    return pa.Table.from_pylist(RECORDS)


def _columns(report: dp.ProfileReport) -> list[dict]:
    return report.to_dict()["columns"]


def _quality(report: dp.ProfileReport) -> dict:
    return report.to_dict()["quality"]


def _profiles(tmp_path: Path) -> dict[str, dp.ProfileReport]:
    jsonl = tmp_path / "records.jsonl"
    jsonl.write_text("".join(json.dumps(record) + "\n" for record in RECORDS), encoding="utf-8")
    parquet = tmp_path / "records.parquet"
    pq.write_table(_table(), parquet)

    jsonl_bytes = jsonl.read_bytes()
    profiles = {
        "pyarrow table": dp.profile(_table()),
        "list of dicts": dp.profile(RECORDS),
        "json bytes": dp.profile(json.dumps(RECORDS).encode(), format="json"),
        "jsonl file": dp.profile(str(jsonl)),
        "parquet file": dp.profile(str(parquet)),
        "parquet bytes": dp.profile(parquet.read_bytes(), format="parquet"),
        "async jsonl bytes": asyncio.run(dp_async.profile_bytes(jsonl_bytes, format="jsonl")),
        "async json bytes": asyncio.run(
            dp_async.profile_bytes(json.dumps(RECORDS).encode(), format="json")
        ),
    }
    try:
        import pandas as pd

        profiles["pandas"] = dp.profile(pd.DataFrame(RECORDS))
    except ImportError:
        pass
    try:
        import polars as pl

        profiles["polars"] = dp.profile(pl.DataFrame(RECORDS))
    except ImportError:
        pass
    return profiles


def test_nested_columns_profile_identically_on_every_path(tmp_path):
    profiles = _profiles(tmp_path)
    baseline_name, baseline = next(iter(profiles.items()))
    nested = [c for c in _columns(baseline) if c["name"] in ("address", "tags")]
    assert [c["data_type"] for c in nested] == ["nested", "nested"]

    for name, report in profiles.items():
        assert _columns(report) == _columns(baseline), f"{name} differs from {baseline_name}"
        assert _quality(report) == _quality(baseline), f"{name} quality differs"


def test_nested_columns_report_their_counts_and_nothing_measured(tmp_path):
    for name, report in _profiles(tmp_path).items():
        for column in ("address", "tags"):
            profile = report[column]
            assert profile.data_type == "nested", name
            assert (profile.total_count, profile.null_count) == (4, 1), name
            assert profile.unique_count is None, name
            assert profile.min_length is None, name
            assert profile.patterns is None, name


def test_a_vector_column_is_nested_until_it_is_profiled_as_one():
    # FixedSizeList<float32> is how embeddings are stored. It gets typed
    # statistics of its own in #663; until then it is not analyzed rather than
    # measured as a display string.
    vectors = pa.FixedSizeListArray.from_arrays(
        pa.array([0.1, 0.2, 0.0, 0.0, 0.5, 0.6], type=pa.float32()), 2
    )
    report = dp.profile(pa.table({"id": [1, 2, 3], "emb": vectors}))
    assert report["emb"].data_type == "nested"
    assert report["emb"].unique_count is None


def test_a_sampled_async_stream_keeps_nested_columns_nested():
    # A fixed-size sample holds rows until the stream ends. Their container
    # positions have to come back with exactly the rows that were kept, or the
    # column is decided on rows that were never analyzed.
    records = [{"id": i, "address": {"n": i}, "tags": [i]} for i in range(50)]
    payload = "".join(json.dumps(record) + "\n" for record in records).encode()
    report = asyncio.run(
        dp_async.profile_bytes(payload, format="jsonl", sampling=dp.SamplingStrategy.reservoir(10))
    )
    for column in ("address", "tags"):
        assert report[column].data_type == "nested"
        assert report[column].total_count == 10


def test_structure_summaries_report_no_distinct_count_for_nested_columns(tmp_path):
    # The approximate flag qualifies a distinct count. A nested column has none,
    # so the flag is absent with it on the JSON path as it is on Parquet.
    jsonl = tmp_path / "records.jsonl"
    jsonl.write_text("".join(json.dumps(record) + "\n" for record in RECORDS), encoding="utf-8")
    parquet = tmp_path / "records.parquet"
    pq.write_table(_table(), parquet)

    for source in (jsonl, parquet):
        columns = {column.name: column for column in dp.analyze_structure(str(source)).columns}
        for name in ("address", "tags"):
            assert columns[name].data_type == "nested", source.suffix
            assert columns[name].unique_count is None, source.suffix
            assert columns[name].distinct_count_approximate is None, source.suffix
