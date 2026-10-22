"""Key uniqueness divides a key's distinct values by its non-null values (#1005).

It used to divide by every row, nulls included, so a nullable key lost
uniqueness for each null the completeness score already counted, and a key
holding no value at all still scored 0.

``tests/key_uniqueness_units.rs`` is the Rust twin.
"""

from __future__ import annotations

import importlib.util
import json
from collections.abc import Callable
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

ROWS = 200

#: Name, the key and the `amount` per row, and the expected `key_uniqueness`,
#: `key_values_checked` and uniqueness score. A distinct `amount` keeps every
#: row unique, so the duplicate component stays at 100. With no key value only
#: that component scores: half the rows repeat, so 50, where counting the key's
#: placeholder 100 beside it would read 75.
CASES: list[
    tuple[str, Callable[[int], str | None], Callable[[int], int], tuple[float, int, float]]
] = [
    (
        "half null, distinct",
        lambda row: f"C{row}" if row < 100 else None,
        lambda row: row,
        (100.0, 100, 100.0),
    ),
    (
        "half null, repeated",
        lambda row: f"C{row % 50}" if row < 100 else None,
        lambda row: row,
        (50.0, 100, 75.0),
    ),
    ("all null", lambda row: None, lambda row: row % 100, (100.0, 0, 50.0)),
]

Table = dict[str, list[Any]]
Route = Callable[[Table], Any]


def _routes(tmp_path: Path) -> dict[str, Route]:
    """Every input route, each profiling the key table."""

    def rows(table: Table) -> list[dict[str, Any]]:
        return [
            dict(zip(table, values, strict=True)) for values in zip(*table.values(), strict=True)
        ]

    def csv_file(engine: str) -> Route:
        def route(table: Table) -> Any:
            path = tmp_path / f"{engine}.csv"
            lines = [
                "customer_id,amount",
                *(f"{row['customer_id'] or ''},{row['amount']}" for row in rows(table)),
            ]
            path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            return dp.profile(str(path), engine=engine)

        return route

    def json_file(table: Table) -> Any:
        path = tmp_path / "table.json"
        path.write_text(json.dumps(rows(table)), encoding="utf-8")
        return dp.profile(str(path))

    def jsonl_file(table: Table) -> Any:
        path = tmp_path / "table.jsonl"
        path.write_text("\n".join(json.dumps(row) for row in rows(table)) + "\n", encoding="utf-8")
        return dp.profile(str(path))

    def arrow_table(table: Table) -> Any:
        import pyarrow as pa

        return pa.table(
            {
                "customer_id": pa.array(table["customer_id"], pa.string()),
                "amount": pa.array(table["amount"], pa.int64()),
            }
        )

    def parquet_file(table: Table) -> Any:
        import pyarrow.parquet as pq

        path = tmp_path / "table.parquet"
        pq.write_table(arrow_table(table), path)
        return dp.profile(str(path))

    def pandas(table: Table) -> Any:
        import pandas as pd

        return dp.profile(pd.DataFrame(table))

    def polars(table: Table) -> Any:
        import polars as pl

        return dp.profile(
            pl.DataFrame(table, schema={"customer_id": pl.String, "amount": pl.Int64})
        )

    routes: dict[str, Route] = {
        "auto": csv_file("auto"),
        "incremental": csv_file("incremental"),
        "columnar": csv_file("columnar"),
        "json": json_file,
        "jsonl": jsonl_file,
        "dict": dp.profile,
        "rows": lambda table: dp.profile(rows(table)),
    }
    # An optional library drops only its own routes; skipping inside a route
    # would skip the whole test and every route it had not reached yet.
    optional = {
        "parquet": ("pyarrow", parquet_file),
        "arrow": ("pyarrow", lambda table: dp.profile(arrow_table(table))),
        "pandas": ("pandas", pandas),
        "polars": ("polars", polars),
    }
    for label, (module, route) in optional.items():
        if importlib.util.find_spec(module) is not None:
            routes[label] = route
    return routes


def test_key_uniqueness_counts_the_key_non_null_values_on_every_path(tmp_path: Path):
    wrong = []
    for name, key, amount, (share, values, score) in CASES:
        table: Table = {
            "customer_id": [key(row) for row in range(ROWS)],
            "amount": [amount(row) for row in range(ROWS)],
        }
        for label, route in _routes(tmp_path).items():
            report = route(table)
            quality = report.to_dict()["quality"]
            uniqueness = quality["uniqueness"]
            observed = (
                uniqueness.get("key_column"),
                uniqueness.get("key_uniqueness"),
                uniqueness.get("key_values_checked"),
                quality["dimension_scores"]["uniqueness"],
            )
            if observed != ("customer_id", share, values, score):
                wrong.append(f"[{label}] {name}: {observed}")
            if report.quality.uniqueness != uniqueness:
                wrong.append(
                    f"[{label}] {name}: accessor {report.quality.uniqueness} != {uniqueness}"
                )
            if report.metric_semantics.get("key_uniqueness_units") != "non_null_values":
                wrong.append(f"[{label}] {name}: metric_semantics {report.metric_semantics}")
    assert not wrong, "\n".join(wrong)
