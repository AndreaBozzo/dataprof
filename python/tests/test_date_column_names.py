"""Date rules read a column name by its words (#869).

The name rule used to match English substrings: ``date`` inside
``candidate_name``, ``time`` inside ``lifetime_tier``. A string column so named
was held to date forms, so clean names scored 0% consistency. The mixed-format
count ran only under those names, so ``data_ordine`` or ``bestelldatum`` holding
ISO and slash dates reported none.

``tests/date_column_names.rs`` is the Rust twin.
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

PEOPLE = ["Anna Rossi", "Luca Bianchi", "Marco Verdi", "Giulia Neri", "Paolo Russo", "Sara Gallo"]

#: Three ISO dates, two slash dates, one junk value: two minority-format values.
MIXED_DATES = ["2024-01-15", "2024-02-20", "2024-03-05", "15/04/2024", "20/05/2024", "unknown"]

NOT_DATE_NAMES = [
    "full_name",
    "candidate_name",
    "validated_by",
    "lifetime_tier",
    "created_by",
    "time_zone",
    "birth_place",
]
DATE_NAMES = ["order_date", "created_at", "startTime", "date_of_birth"]
ANY_LANGUAGE_DATE_NAMES = [
    "order_date",
    "date_de_commande",
    "data_ordine",
    "bestelldatum",
    "fecha_pedido",
]


def _routes(tmp_path: Path) -> dict[str, Callable[[str, list[str]], Any]]:
    """Every input route, each profiling one string column."""

    def csv_file(engine: str) -> Callable[[str, list[str]], Any]:
        def run(name: str, values: list[str]) -> Any:
            path = tmp_path / f"{name}.csv"
            path.write_text("\n".join([name, *values]) + "\n", encoding="utf-8")
            return dp.profile(str(path), engine=engine)

        return run

    def json_file(name: str, values: list[str]) -> Any:
        path = tmp_path / f"{name}.json"
        path.write_text(json.dumps([{name: v} for v in values]), encoding="utf-8")
        return dp.profile(str(path))

    def jsonl_file(name: str, values: list[str]) -> Any:
        path = tmp_path / f"{name}.jsonl"
        path.write_text("\n".join(json.dumps({name: v}) for v in values) + "\n", encoding="utf-8")
        return dp.profile(str(path))

    def parquet_file(name: str, values: list[str]) -> Any:
        import pyarrow as pa
        import pyarrow.parquet as pq

        path = tmp_path / f"{name}.parquet"
        pq.write_table(pa.table({name: pa.array(values, pa.string())}), path)
        return dp.profile(str(path))

    def arrow(name: str, values: list[str]) -> Any:
        import pyarrow as pa

        return dp.profile(pa.table({name: pa.array(values, pa.string())}))

    def pandas(name: str, values: list[str]) -> Any:
        import pandas as pd

        return dp.profile(pd.DataFrame({name: values}))

    def polars(name: str, values: list[str]) -> Any:
        import polars as pl

        return dp.profile(pl.DataFrame({name: values}))

    routes: dict[str, Callable[[str, list[str]], Any]] = {
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


def _consistency(report: Any) -> dict[str, Any]:
    return report.to_dict()["quality"]["consistency"]


def test_names_that_merely_contain_a_date_word_are_not_held_to_dates(tmp_path: Path):
    wrong = []
    for label, route in _routes(tmp_path).items():
        for name in NOT_DATE_NAMES:
            score = _consistency(route(name, PEOPLE))["data_type_consistency"]
            if score != 100.0:
                wrong.append(f"[{label}] {name}: data_type_consistency {score}")
    assert not wrong, "\n".join(wrong)


def test_a_name_with_a_date_word_still_holds_its_column_to_dates(tmp_path: Path):
    wrong = []
    for label, route in _routes(tmp_path).items():
        for name in DATE_NAMES:
            score = _consistency(route(name, PEOPLE))["data_type_consistency"]
            if score != 0.0:
                wrong.append(f"[{label}] {name}: data_type_consistency {score}")
    assert not wrong, "\n".join(wrong)


def test_mixed_date_formats_are_counted_under_any_name(tmp_path: Path):
    wrong = []
    for label, route in _routes(tmp_path).items():
        for name in ANY_LANGUAGE_DATE_NAMES:
            violations = _consistency(route(name, MIXED_DATES))["format_violations"]
            if violations != 2:
                wrong.append(f"[{label}] {name}: format_violations {violations}")
    assert not wrong, "\n".join(wrong)
