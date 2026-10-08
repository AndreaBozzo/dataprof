"""Accuracy range rules read a column name by its words (#871).

The rules used to match English substrings: ``age`` inside ``average_price``
and ``mileage``, ``rate`` inside ``migrated_rows``, ``count`` inside
``discount``. Clean numbers under those names were counted as range
violations and scored 0% accuracy.

``tests/accuracy_range_names.rs`` is the Rust twin.
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

#: Names holding a rule word only inside a longer word, each with clean values
#: that the rule it used to match would reject.
NOT_RULE_NAMES = {
    "average_price": [200, 250, 300, 180],
    "mileage": [120000, 85000, 43000, 99000],
    "page_views": [310, 1200, 450, 980],
    "migrated_rows": [5000, 7000, 6500, 8000],
    "generated_tokens": [512, 1024, 2048, 700],
    "discount": [-5, -10, -2, -1],
    "account_number": [-5, 10, 20, 30],
}

#: Names whose words carry a rule, each with two values that break it.
RULE_NAMES = {
    "age": [30, 200, 45, 300],
    "customer_age": [30, 200, 45, 300],
    "conversion_rate": [12, 250, 40, 180],
    "item_count": [3, -1, 5, -2],
    "birth_year": [1985, 1850, 1990, 2500],
    "customerAge": [30, 200, 45, 300],
}


def _routes(tmp_path: Path) -> dict[str, Callable[[str, list[int]], Any]]:
    """Every input route, each profiling one integer column."""

    def csv_file(engine: str) -> Callable[[str, list[int]], Any]:
        def run(name: str, values: list[int]) -> Any:
            path = tmp_path / f"{name}.csv"
            path.write_text("\n".join([name, *map(str, values)]) + "\n", encoding="utf-8")
            return dp.profile(str(path), engine=engine)

        return run

    def json_file(name: str, values: list[int]) -> Any:
        path = tmp_path / f"{name}.json"
        path.write_text(json.dumps([{name: v} for v in values]), encoding="utf-8")
        return dp.profile(str(path))

    def jsonl_file(name: str, values: list[int]) -> Any:
        path = tmp_path / f"{name}.jsonl"
        path.write_text("\n".join(json.dumps({name: v}) for v in values) + "\n", encoding="utf-8")
        return dp.profile(str(path))

    def parquet_file(name: str, values: list[int]) -> Any:
        import pyarrow as pa
        import pyarrow.parquet as pq

        path = tmp_path / f"{name}.parquet"
        pq.write_table(pa.table({name: pa.array(values, pa.int64())}), path)
        return dp.profile(str(path))

    def arrow(name: str, values: list[int]) -> Any:
        import pyarrow as pa

        return dp.profile(pa.table({name: pa.array(values, pa.int64())}))

    def pandas(name: str, values: list[int]) -> Any:
        import pandas as pd

        return dp.profile(pd.DataFrame({name: values}))

    def polars(name: str, values: list[int]) -> Any:
        import polars as pl

        return dp.profile(pl.DataFrame({name: values}))

    routes: dict[str, Callable[[str, list[int]], Any]] = {
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
    optional = {"parquet": ("pyarrow", parquet_file), "arrow": ("pyarrow", arrow)}
    optional |= {"pandas": ("pandas", pandas), "polars": ("polars", polars)}
    for label, (module, route) in optional.items():
        if importlib.util.find_spec(module) is not None:
            routes[label] = route
    return routes


def test_names_that_merely_contain_a_rule_word_are_not_held_to_its_range(tmp_path: Path):
    wrong = []
    for label, route in _routes(tmp_path).items():
        for name, values in NOT_RULE_NAMES.items():
            quality = route(name, values).to_dict()["quality"]
            violations = quality["accuracy"]["range_violations"]
            score = quality["dimension_scores"]["accuracy"]
            if violations != 0 or score != 100.0:
                wrong.append(f"[{label}] {name}: range_violations {violations}, accuracy {score}")
    assert not wrong, "\n".join(wrong)


def test_a_name_with_a_rule_word_still_holds_its_column_to_the_range(tmp_path: Path):
    wrong = []
    for label, route in _routes(tmp_path).items():
        for name, values in RULE_NAMES.items():
            violations = route(name, values).to_dict()["quality"]["accuracy"]["range_violations"]
            if violations != 2:
                wrong.append(f"[{label}] {name}: range_violations {violations}")
    assert not wrong, "\n".join(wrong)
