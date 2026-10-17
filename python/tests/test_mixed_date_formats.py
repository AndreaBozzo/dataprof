"""Mixed date formats are counted over every value of a column (#873).

``format_violations`` used to read only the first 50 non-empty values of each
column while ``values_checked`` counted all of them, so a minority format after
the first 50 values went unreported.

``tests/mixed_date_formats.rs`` is the Rust twin.
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

NAME = "order_date"


def _iso(index: int) -> str:
    return f"2024-03-{1 + index % 28:02}"


def _slash(index: int) -> str:
    return f"{1 + index % 28:02}/03/2024"


#: 300 values, each holding 50 in the minority form.
FIXTURES = {
    "50 ISO, then 250 slash": [_iso(i) if i < 50 else _slash(i) for i in range(300)],
    "one slash in six": [_slash(i) if i % 6 == 0 else _iso(i) for i in range(300)],
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


def test_a_minority_date_format_is_counted_over_every_value(tmp_path: Path):
    wrong = []
    for label, route in _routes(tmp_path).items():
        for index, (fixture, values) in enumerate(FIXTURES.items()):
            consistency = route(f"f{index}", values).to_dict()["quality"]["consistency"]
            found = (consistency["format_violations"], consistency["values_checked"])
            if found != (50, 300):
                wrong.append(f"[{label}] {fixture}: format_violations {found[0]} of {found[1]}")
    assert not wrong, "\n".join(wrong)
