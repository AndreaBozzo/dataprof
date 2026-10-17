"""Every engine types a column ``date`` from the same share of its values (#915).

The streaming inferrer counted date-like values over only the first 100 values
of a column's sample, while ``infer_type`` counts all of them, so a column that
opened with dates was a date on the streaming engines and text everywhere else,
and one that opened with words was the reverse.

``tests/date_share_every_value.rs`` is the Rust twin.
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

NAME = "event"


def _date(index: int) -> str:
    return f"2024-03-{1 + index % 28:02}"


def _word(index: int) -> str:
    return f"word{index}"


#: 1,000 values each, with the type every route should agree on.
FIXTURES = {
    "150 dates, then 850 words": (
        [_date(i) if i < 150 else _word(i) for i in range(1_000)],
        "string",
    ),
    "100 words, then 900 dates": (
        [_word(i) if i < 100 else _date(i) for i in range(1_000)],
        "date",
    ),
}

Route = Callable[[str, list[str]], Any]


def _routes(tmp_path: Path) -> dict[str, Route]:
    """Every input route, each profiling one string column."""

    def csv_file(engine: str) -> Route:
        def run(label: str, values: list[str]) -> Any:
            path = tmp_path / f"{label}-{engine}.csv"
            path.write_text("\n".join([NAME, *values]) + "\n", encoding="utf-8")
            return dp.profile(str(path), engine=engine)

        return run

    def json_file(label: str, values: list[str]) -> Any:
        path = tmp_path / f"{label}.json"
        path.write_text(json.dumps([{NAME: v} for v in values]), encoding="utf-8")
        return dp.profile(str(path))

    def jsonl_file(label: str, values: list[str]) -> Any:
        path = tmp_path / f"{label}.jsonl"
        path.write_text("\n".join(json.dumps({NAME: v}) for v in values) + "\n", encoding="utf-8")
        return dp.profile(str(path))

    def parquet_file(label: str, values: list[str]) -> Any:
        import pyarrow as pa
        import pyarrow.parquet as pq

        path = tmp_path / f"{label}.parquet"
        pq.write_table(pa.table({NAME: pa.array(values, pa.string())}), path)
        return dp.profile(str(path))

    def arrow(label: str, values: list[str]) -> Any:
        import pyarrow as pa

        return dp.profile(pa.table({NAME: pa.array(values, pa.string())}))

    def pandas(label: str, values: list[str]) -> Any:
        import pandas as pd

        return dp.profile(pd.DataFrame({NAME: values}))

    def polars(label: str, values: list[str]) -> Any:
        import polars as pl

        return dp.profile(pl.DataFrame({NAME: values}))

    routes: dict[str, Route] = {
        "auto": csv_file("auto"),
        "incremental": csv_file("incremental"),
        "columnar": csv_file("columnar"),
        "json": json_file,
        "jsonl": jsonl_file,
        "dict": lambda label, values: dp.profile({NAME: values}),
        "rows": lambda label, values: dp.profile([{NAME: v} for v in values]),
    }
    # An optional library drops only its own routes; skipping inside a route
    # would skip the whole test and every route it had not reached yet.
    optional = {"parquet": ("pyarrow", parquet_file), "arrow": ("pyarrow", arrow)}
    optional |= {"pandas": ("pandas", pandas), "polars": ("polars", polars)}
    for label, (module, route) in optional.items():
        if importlib.util.find_spec(module) is not None:
            routes[label] = route
    return routes


def test_every_route_types_a_column_from_the_date_share_of_all_its_values(tmp_path: Path):
    wrong = []
    for label, route in _routes(tmp_path).items():
        for index, (fixture, (values, expected)) in enumerate(FIXTURES.items()):
            data_type = route(f"f{index}", values).to_dict()["columns"][0]["data_type"]
            if data_type != expected:
                wrong.append(f"[{label}] {fixture}: {data_type}, expected {expected}")
    assert not wrong, "\n".join(wrong)
