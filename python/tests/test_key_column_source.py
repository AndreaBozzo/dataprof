"""Key uniqueness measures only a declared key (#1065).

With no key declared, the first column whose name had an ``id``, ``key`` or
``pk`` word was the key. A name cannot tell a table's key from a foreign key,
so a ``customer_id`` listed before ``order_id`` was scored as the table key,
and the same clean table scored differently by column order.

``tests/key_column_source.rs`` is the Rust twin.
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

#: The same table in two column orders: the foreign key last, then first.
ORDERS = [("order_id", "customer_id", "amount"), ("customer_id", "order_id", "amount")]

#: A declaration and the key column, ``key_uniqueness`` and uniqueness score it
#: must give in either order. ``order_id`` is unique, ``customer_id`` repeats
#: over 80 customers, and a distinct ``amount`` keeps every row unique.
CASES: list[tuple[list[str], tuple[str | None, float, float]]] = [
    ([], (None, 100.0, 100.0)),
    (["order_id"], ("order_id", 100.0, 100.0)),
    (["customer_id"], ("customer_id", 40.0, 70.0)),
]

Table = dict[str, list[Any]]
Route = Callable[[Table, list[str]], Any]


def _table(columns: tuple[str, ...]) -> Table:
    values: dict[str, Callable[[int], Any]] = {
        "order_id": lambda row: f"O{row}",
        "customer_id": lambda row: f"C{row % 80}",
        "amount": lambda row: row,
    }
    return {column: [values[column](row) for row in range(ROWS)] for column in columns}


def _routes(tmp_path: Path) -> dict[str, Route]:
    """Every input route, each profiling the table with a declaration."""

    def rows(table: Table) -> list[dict[str, Any]]:
        return [
            dict(zip(table, values, strict=True)) for values in zip(*table.values(), strict=True)
        ]

    def csv_file(engine: str) -> Route:
        def route(table: Table, declared: list[str]) -> Any:
            path = tmp_path / f"{engine}.csv"
            lines = [",".join(table), *(",".join(map(str, row.values())) for row in rows(table))]
            path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            return dp.profile(str(path), engine=engine, identifier_columns=declared)

        return route

    def json_file(table: Table, declared: list[str]) -> Any:
        path = tmp_path / "table.json"
        path.write_text(json.dumps(rows(table)), encoding="utf-8")
        return dp.profile(str(path), identifier_columns=declared)

    def jsonl_file(table: Table, declared: list[str]) -> Any:
        path = tmp_path / "table.jsonl"
        path.write_text("\n".join(json.dumps(row) for row in rows(table)) + "\n", encoding="utf-8")
        return dp.profile(str(path), identifier_columns=declared)

    def arrow_table(table: Table) -> Any:
        import pyarrow as pa

        return pa.table(
            {
                column: pa.array(values, pa.int64() if column == "amount" else pa.string())
                for column, values in table.items()
            }
        )

    def parquet_file(table: Table, declared: list[str]) -> Any:
        import pyarrow.parquet as pq

        path = tmp_path / "table.parquet"
        pq.write_table(arrow_table(table), path)
        return dp.profile(str(path), identifier_columns=declared)

    def pandas(table: Table, declared: list[str]) -> Any:
        import pandas as pd

        return dp.profile(pd.DataFrame(table), identifier_columns=declared)

    def polars(table: Table, declared: list[str]) -> Any:
        import polars as pl

        return dp.profile(pl.DataFrame(table), identifier_columns=declared)

    routes: dict[str, Route] = {
        "auto": csv_file("auto"),
        "incremental": csv_file("incremental"),
        "columnar": csv_file("columnar"),
        "json": json_file,
        "jsonl": jsonl_file,
        "dict": lambda table, declared: dp.profile(table, identifier_columns=declared),
        "rows": lambda table, declared: dp.profile(rows(table), identifier_columns=declared),
    }
    # An optional library drops only its own routes; skipping inside a route
    # would skip the whole test and every route it had not reached yet.
    optional: dict[str, tuple[str, Route]] = {
        "parquet": ("pyarrow", parquet_file),
        "arrow": (
            "pyarrow",
            lambda table, declared: dp.profile(arrow_table(table), identifier_columns=declared),
        ),
        "pandas": ("pandas", pandas),
        "polars": ("polars", polars),
    }
    for label, (module, route) in optional.items():
        if importlib.util.find_spec(module) is not None:
            routes[label] = route
    return routes


def test_only_a_declared_key_is_measured_whatever_the_column_order(tmp_path: Path):
    wrong = []
    for declared, (key, share, score) in CASES:
        first_scores: dict[str, Any] = {}
        for columns in ORDERS:
            for label, route in _routes(tmp_path).items():
                report = route(_table(columns), declared)
                quality = report.to_dict()["quality"]
                uniqueness = quality["uniqueness"]
                observed = (
                    uniqueness.get("key_column"),
                    uniqueness.get("key_uniqueness"),
                    quality["dimension_scores"]["uniqueness"],
                )
                if observed != (key, share, score):
                    wrong.append(f"[{label}] {declared} {columns}: {observed}")
                scores = (quality["overall_score"], quality["dimension_scores"])
                if first_scores.setdefault(label, scores) != scores:
                    wrong.append(
                        f"[{label}] {declared}: scores move with column order: "
                        f"{first_scores[label]} vs {scores}"
                    )
                if report.metric_semantics.get("key_column_source") != "declared":
                    wrong.append(f"[{label}] metric_semantics {report.metric_semantics}")
    assert not wrong, "\n".join(wrong)
