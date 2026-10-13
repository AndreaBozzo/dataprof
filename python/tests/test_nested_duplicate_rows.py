"""Duplicate rows holding a nested value count the same on every path (#767).

The JSON readers rendered an object in its source key order, so equal records
with their keys in a different order were not duplicates. Arrow and Parquet
rendered a nested value as its display string, which matches ``["p, q"]`` with
``["p", "q"]`` and ``[None]`` with ``[]``.

``tests/nested_duplicate_rows.rs`` is the Rust twin.
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

#: (name, records, duplicate rows, whether Arrow can hold the records)
CASES = [
    (
        "key order",
        [{"id": 1, "a": {"x": 1, "y": 2}}, {"id": 1, "a": {"y": 2, "x": 1}}],
        1,
        True,
    ),
    (
        "nested key order",
        [
            {"id": 1, "a": {"b": {"x": 1, "y": [1, 2]}, "c": "k"}},
            {"id": 1, "a": {"c": "k", "b": {"y": [1, 2], "x": 1}}},
        ],
        1,
        True,
    ),
    (
        "comma inside a list element",
        [{"id": 1, "tags": ["p, q"]}, {"id": 1, "tags": ["p", "q"]}],
        0,
        True,
    ),
    (
        "null element against an empty list",
        [{"id": 1, "tags": ["p"]}, {"id": 2, "tags": [None]}, {"id": 2, "tags": []}],
        0,
        True,
    ),
    (
        "null child repeated",
        [
            {"id": 1, "a": {"x": None, "y": "1"}},
            {"id": 1, "a": {"x": None, "y": "1"}},
            {"id": 1, "a": {"x": "1", "y": None}},
        ],
        1,
        True,
    ),
    (
        "empty object against an empty list",
        [{"id": 1, "a": {}}, {"id": 1, "a": []}],
        0,
        False,
    ),
]


def _routes(records: list[dict], arrow: bool, tmp_path: Path) -> dict[str, dp.ProfileReport]:
    jsonl_bytes = "".join(json.dumps(record) + "\n" for record in records).encode()
    json_bytes = json.dumps(records).encode()
    jsonl = tmp_path / "records.jsonl"
    jsonl.write_bytes(jsonl_bytes)
    routes = {
        "list of dicts": dp.profile(records),
        "json bytes": dp.profile(json_bytes, format="json"),
        "jsonl file": dp.profile(str(jsonl)),
        "async jsonl bytes": asyncio.run(dp_async.profile_bytes(jsonl_bytes, format="jsonl")),
        "async json bytes": asyncio.run(dp_async.profile_bytes(json_bytes, format="json")),
    }
    if arrow:
        table = pa.Table.from_pylist(records)
        parquet = tmp_path / "records.parquet"
        pq.write_table(table, parquet)
        routes["pyarrow table"] = dp.profile(table)
        routes["parquet file"] = dp.profile(str(parquet))
        routes["parquet bytes"] = dp.profile(parquet.read_bytes(), format="parquet")
        try:
            import polars as pl

            routes["polars"] = dp.profile(pl.from_arrow(table))
        except ImportError:
            pass
        try:
            import pandas as pd

            # pandas reaches dataprof through Arrow, so it cannot hold them either.
            routes["pandas"] = dp.profile(pd.DataFrame(records))
        except ImportError:
            pass
    return routes


def test_equal_nested_rows_are_duplicates_on_every_path(tmp_path):
    # Every wrong (case, route) at once, so one run shows which routes each
    # part of the fix guards.
    wrong = []
    for index, (name, records, duplicates, arrow) in enumerate(CASES):
        case_dir = tmp_path / str(index)
        case_dir.mkdir()
        for route, report in _routes(records, arrow, case_dir).items():
            found = report.to_dict()["quality"]["uniqueness"]["duplicate_rows"]
            if found != duplicates:
                wrong.append(f"{name} / {route}: {found}, expected {duplicates}")
    assert not wrong, "\n".join(wrong)
