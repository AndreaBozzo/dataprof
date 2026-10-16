"""Duplicate rows holding an Arrow map compare entries, not their order (#943).

A row signature encoded a map's entries in stored order. Arrow and Parquet do
not give that order a meaning, so two rows holding the same entries in a
different order were not counted as duplicates.

``tests/arrow_map_duplicate_rows.rs`` is the Rust twin.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

try:
    import dataprof as dp
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

pa = pytest.importorskip("pyarrow", reason="pyarrow builds the map fixtures")
pq = pytest.importorskip("pyarrow.parquet", reason="pyarrow writes the Parquet fixture")

MAP = pa.map_(pa.string(), pa.int64())

#: (name, value type, one value per row, duplicate rows). Every row has id 1.
CASES: list[tuple[str, Any, list[Any], int]] = [
    (
        "entries in a different order",
        MAP,
        [[("a", 1), ("b", 2)], [("b", 2), ("a", 1)]],
        1,
    ),
    (
        "same keys, different values",
        MAP,
        [[("a", 1), ("b", 2)], [("b", 1), ("a", 2)]],
        0,
    ),
    ("a null map against an empty map", MAP, [None, []], 0),
    (
        "maps inside a list",
        pa.list_(MAP),
        [[[("a", 1), ("b", 2)], [("c", 3)]], [[("b", 2), ("a", 1)], [("c", 3)]]],
        1,
    ),
    (
        "a map inside a struct",
        pa.struct([("m", MAP)]),
        [{"m": [("a", 1), ("b", 2)]}, {"m": [("b", 2), ("a", 1)]}],
        1,
    ),
    (
        "a map inside a map value",
        pa.map_(pa.string(), MAP),
        [
            [("k", [("a", 1), ("b", 2)]), ("j", [])],
            [("j", []), ("k", [("b", 2), ("a", 1)])],
        ],
        1,
    ),
]


def _table(value_type: Any, values: list[Any]) -> Any:
    return pa.table(
        {
            "id": pa.array([1] * len(values), pa.int64()),
            "value": pa.array(values, value_type),
        }
    )


def _routes(value_type: Any, values: list[Any], tmp_path: Path) -> dict[str, Any]:
    table = _table(value_type, values)
    parquet = tmp_path / "maps.parquet"
    pq.write_table(table, parquet)
    # A slice starts its map offsets past zero; the leading row is left out.
    padded = _table(value_type, [values[0], *values])
    routes = {
        "pyarrow table": lambda: dp.profile(table),
        "sliced pyarrow table": lambda: dp.profile(padded.slice(1)),
        "parquet file": lambda: dp.profile(str(parquet)),
        "parquet bytes": lambda: dp.profile(parquet.read_bytes(), format="parquet"),
    }
    try:
        import pandas as pd

        frame = table.to_pandas(types_mapper=pd.ArrowDtype)
        routes["pandas"] = lambda: dp.profile(frame)
    except ImportError:
        pass
    try:
        import polars as pl

        frame = pl.from_arrow(table)
        routes["polars"] = lambda: dp.profile(frame)
    except ImportError:
        pass
    return routes


def test_maps_compare_by_their_entries_not_their_order(tmp_path: Path):
    # Every wrong (case, route) at once, so one run shows which routes and
    # shapes the fix reaches.
    wrong = []
    for index, (name, value_type, values, duplicates) in enumerate(CASES):
        case_dir = tmp_path / str(index)
        case_dir.mkdir()
        for route, run in _routes(value_type, values, case_dir).items():
            found = run().to_dict()["quality"]["uniqueness"]["duplicate_rows"]
            if found != duplicates:
                wrong.append(f"{name} / {route}: {found}, expected {duplicates}")
    assert not wrong, "\n".join(wrong)
