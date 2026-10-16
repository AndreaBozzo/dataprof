"""Start/end pairs declared next to ``temporal_columns`` (#942).

Timeliness found pairs only from role words in the names, so
``valid_from``/``valid_until``, ``checkin``/``checkout`` or names in another
language were never compared. A declared pair is compared as given on every
engine and input path, and an unknown name in one is reported like any other
hint.

``tests/timeliness_declared_pairs.rs`` is the Rust twin.
"""

from __future__ import annotations

import datetime as dt
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

START, END = "valid_from", "valid_until"
#: The start of every row.
STARTS = ["2024-03-15", "2024-04-20", "2024-05-10"]
#: The end, before the start on every row, so the pair counts three violations.
ENDS = ["2024-02-01", "2024-03-01", "2024-03-20"]

Route = Callable[..., Any]


def _dates(values: list[str]) -> list[dt.date]:
    return [dt.date.fromisoformat(value) for value in values]


def _records() -> list[dict[str, str]]:
    return [{START: start, END: end} for start, end in zip(STARTS, ENDS, strict=True)]


def _csv(tmp_path: Path) -> Path:
    path = tmp_path / "validity.csv"
    rows = [f"{START},{END}", *(f"{a},{b}" for a, b in zip(STARTS, ENDS, strict=True))]
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return path


def _routes(tmp_path: Path) -> dict[str, Route]:
    """Every input route, each profiling the two date columns with ``**hints``."""

    def csv_file(engine: str) -> Route:
        return lambda **hints: dp.profile(str(_csv(tmp_path)), engine=engine, **hints)

    def json_file(**hints: Any) -> Any:
        path = tmp_path / "validity.json"
        path.write_text(json.dumps(_records()), encoding="utf-8")
        return dp.profile(str(path), **hints)

    def jsonl_file(**hints: Any) -> Any:
        path = tmp_path / "validity.jsonl"
        path.write_text("\n".join(json.dumps(r) for r in _records()) + "\n", encoding="utf-8")
        return dp.profile(str(path), **hints)

    def arrow_table() -> Any:
        import pyarrow as pa

        return pa.table(
            {
                START: pa.array(_dates(STARTS), pa.date32()),
                END: pa.array(_dates(ENDS), pa.date32()),
            }
        )

    def parquet_file(**hints: Any) -> Any:
        import pyarrow.parquet as pq

        path = tmp_path / "validity.parquet"
        pq.write_table(arrow_table(), path)
        return dp.profile(str(path), **hints)

    def arrow(**hints: Any) -> Any:
        return dp.profile(arrow_table(), **hints)

    def pandas(**hints: Any) -> Any:
        import pandas as pd

        return dp.profile(pd.DataFrame({START: STARTS, END: ENDS}), **hints)

    def polars(**hints: Any) -> Any:
        import polars as pl

        return dp.profile(pl.DataFrame({START: STARTS, END: ENDS}), **hints)

    def builder(**hints: Any) -> Any:
        profiler = dp.Profiler().temporal_pairs(hints["temporal_pairs"])
        return profiler.profile(str(_csv(tmp_path)))

    routes: dict[str, Route] = {
        "auto": csv_file("auto"),
        "incremental": csv_file("incremental"),
        "columnar": csv_file("columnar"),
        "json": json_file,
        "jsonl": jsonl_file,
        "dict": lambda **hints: dp.profile({START: STARTS, END: ENDS}, **hints),
        "rows": lambda **hints: dp.profile(_records(), **hints),
        "profiler": builder,
    }
    # An optional library drops only its own routes; skipping inside a route
    # would skip the whole test and every route it had not reached yet.
    optional = {"parquet": ("pyarrow", parquet_file), "arrow": ("pyarrow", arrow)}
    optional |= {"pandas": ("pandas", pandas), "polars": ("polars", polars)}
    for label, (module, route) in optional.items():
        if importlib.util.find_spec(module) is not None:
            routes[label] = route
    return routes


def _ordering(report: Any) -> tuple[int, int]:
    timeliness = report.to_dict()["quality"]["timeliness"]
    return timeliness["temporal_pairs_checked"], timeliness["temporal_violations"]


def test_a_declared_pair_is_compared_on_every_path(tmp_path: Path):
    wrong = []
    for label, route in _routes(tmp_path).items():
        for pairs, expected in (([], (0, 0)), ([(START, END)], (3, 3))):
            found = _ordering(route(temporal_pairs=pairs))
            if found != expected:
                wrong.append(f"[{label}] declared {pairs}: {found}, expected {expected}")
    assert not wrong, "\n".join(wrong)


def test_a_pair_given_as_a_list_works_like_a_tuple(tmp_path: Path):
    report = dp.profile(str(_csv(tmp_path)), temporal_pairs=[[START, END]])  # ty: ignore[invalid-argument-type]
    assert _ordering(report) == (3, 3)


def test_an_unknown_column_in_a_declared_pair_is_rejected(tmp_path: Path):
    for label, route in _routes(tmp_path).items():
        with pytest.raises(ValueError) as caught:
            route(temporal_pairs=[(START, "valid_untill")])
        message = str(caught.value)
        assert "'valid_untill'" in message, label
        assert "temporal_pairs" in message, label


@pytest.mark.parametrize("pair", [[START], [START, END, END]])
def test_an_entry_that_is_not_two_names_is_rejected(tmp_path: Path, pair: list[str]):
    with pytest.raises(ValueError, match="temporal_pairs entries must be"):
        dp.profile(str(_csv(tmp_path)), temporal_pairs=[pair])  # ty: ignore[invalid-argument-type]


def test_config_reports_the_declared_pairs():
    config = dp.ProfilerConfig(temporal_pairs=[(START, END)])
    assert config.temporal_pairs == [(START, END)]
