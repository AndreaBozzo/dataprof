"""The outlier component is measured over the values of the columns that reached
`outlier_min_samples`, and is absent when none did (#1005).

An untested source used to report `outlier_ratio` 0 and score accuracy 100.
Accuracy is now assessed only when a component had units.

``tests/outlier_component_units.rs`` is the Rust twin.
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

#: Name, column, values, and the expected `outlier_values_checked` and accuracy
#: score, or None for an unassessed dimension. The default `outlier_min_samples`
#: is 4.
CASES: list[tuple[str, str, list[int], tuple[int, float] | None]] = [
    # Too few values for the test and no rule: nothing measured.
    ("untested, no rule", "amount", [1, 2, 1_000_000], None),
    # Too few values, but the percentage rule applies: 1 violating value of 3.
    ("untested, ruled", "discount_percent", [10, 20, 150], (0, 66.67)),
    # Four values: tested, and 1,000,000 is outside the fences.
    ("tested", "amount", [1, 2, 3, 1_000_000], (4, 75.0)),
]

Route = Callable[[str, list[int]], Any]


def _routes(tmp_path: Path) -> dict[str, Route]:
    """Every input route, each profiling one integer column."""

    def csv_file(engine: str) -> Route:
        def route(name: str, values: list[int]) -> Any:
            path = tmp_path / f"{engine}.csv"
            path.write_text("\n".join([name, *map(str, values)]) + "\n", encoding="utf-8")
            return dp.profile(str(path), engine=engine)

        return route

    def json_file(name: str, values: list[int]) -> Any:
        path = tmp_path / "table.json"
        path.write_text(json.dumps([{name: v} for v in values]), encoding="utf-8")
        return dp.profile(str(path))

    def jsonl_file(name: str, values: list[int]) -> Any:
        path = tmp_path / "table.jsonl"
        path.write_text("\n".join(json.dumps({name: v}) for v in values) + "\n", encoding="utf-8")
        return dp.profile(str(path))

    def arrow_table(name: str, values: list[int]) -> Any:
        import pyarrow as pa

        return pa.table({name: pa.array(values, pa.int64())})

    def parquet_file(name: str, values: list[int]) -> Any:
        import pyarrow.parquet as pq

        path = tmp_path / "table.parquet"
        pq.write_table(arrow_table(name, values), path)
        return dp.profile(str(path))

    def pandas(name: str, values: list[int]) -> Any:
        import pandas as pd

        return dp.profile(pd.DataFrame({name: values}))

    def polars(name: str, values: list[int]) -> Any:
        import polars as pl

        return dp.profile(pl.DataFrame({name: values}))

    routes: dict[str, Route] = {
        "auto": csv_file("auto"),
        "incremental": csv_file("incremental"),
        "columnar": csv_file("columnar"),
        "json": json_file,
        "jsonl": jsonl_file,
        "dict": lambda name, values: dp.profile({name: values}),
        "rows": lambda name, values: dp.profile([{name: v} for v in values]),
    }
    # An optional library drops only its own routes; skipping inside a route
    # would skip the whole test and every route it had not reached yet.
    optional = {
        "parquet": ("pyarrow", parquet_file),
        "arrow": ("pyarrow", lambda name, values: dp.profile(arrow_table(name, values))),
        "pandas": ("pandas", pandas),
        "polars": ("polars", polars),
    }
    for label, (module, route) in optional.items():
        if importlib.util.find_spec(module) is not None:
            routes[label] = route
    return routes


def test_an_untested_outlier_component_is_absent_on_every_path(tmp_path: Path):
    wrong = []
    for case, column, values, expected in CASES:
        for label, route in _routes(tmp_path).items():
            report = route(column, values)
            quality = report.to_dict()["quality"]
            accuracy = quality.get("accuracy")
            observed = (
                None if accuracy is None else accuracy.get("outlier_values_checked"),
                quality["dimension_scores"]["accuracy"],
            )
            if observed != (expected or (None, None)):
                wrong.append(f"[{label}] {case}: {observed}")
            if report.quality.accuracy != accuracy:
                wrong.append(f"[{label}] {case}: accessor {report.quality.accuracy} != {accuracy}")
            if report.metric_semantics.get("outlier_units") != "tested_values":
                wrong.append(f"[{label}] {case}: metric_semantics {report.metric_semantics}")
    assert not wrong, "\n".join(wrong)
