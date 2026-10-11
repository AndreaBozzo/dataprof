"""Timeliness pairs a start column with an end column by the words of their names (#882).

The roles used to match as substrings, so ``end`` inside ``send_date`` or
``weekend_date`` and ``to_date`` inside ``photo_date`` paired two unrelated
dates. Every row where the second date fell before the first counted as a
temporal violation and lowered the timeliness score.

``tests/timeliness_role_words.rs`` is the Rust twin.
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

#: The first column of every fixture.
FIRST = ["2024-03-15", "2024-04-20", "2024-05-10"]
#: The second column, one to two months before the first on every row, so a
#: pair that forms counts three violations.
SECOND = ["2024-02-01", "2024-03-01", "2024-03-20"]

#: Column names that hold a role only inside a longer word.
NOT_PAIRS = [
    ("start_date", "send_date"),
    ("start_date", "weekend_date"),
    ("restart_date", "end_date"),
    ("from_date", "photo_date"),
]

#: Column names whose words carry the roles: the pairs the issue lists, the
#: inflected and all-lowercase forms the substring match also paired, and the
#: camelCase forms of the two-word roles, which it did not.
PAIRS = [
    ("start_date", "end_date"),
    ("created_at", "updated_at"),
    ("begin_date", "end_date"),
    ("from_date", "to_date"),
    ("startDate", "endDate"),
    ("starts_at", "ends_at"),
    ("startdate", "enddate"),
    ("beginDate", "endDate"),
    ("fromDate", "toDate"),
]

Route = Callable[[str, str], Any]


def _dates(values: list[str]) -> list[dt.date]:
    return [dt.date.fromisoformat(value) for value in values]


def _routes(tmp_path: Path) -> dict[str, Route]:
    """Every input route, each profiling the two date columns."""

    def csv_file(engine: str) -> Route:
        def run(first: str, second: str) -> Any:
            path = tmp_path / f"{first}-{second}-{engine}.csv"
            rows = [f"{first},{second}", *(f"{a},{b}" for a, b in zip(FIRST, SECOND, strict=True))]
            path.write_text("\n".join(rows) + "\n", encoding="utf-8")
            return dp.profile(str(path), engine=engine)

        return run

    def records(first: str, second: str) -> list[dict[str, str]]:
        return [{first: a, second: b} for a, b in zip(FIRST, SECOND, strict=True)]

    def json_file(first: str, second: str) -> Any:
        path = tmp_path / f"{first}-{second}.json"
        path.write_text(json.dumps(records(first, second)), encoding="utf-8")
        return dp.profile(str(path))

    def jsonl_file(first: str, second: str) -> Any:
        path = tmp_path / f"{first}-{second}.jsonl"
        lines = (json.dumps(record) for record in records(first, second))
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return dp.profile(str(path))

    def arrow_table(first: str, second: str) -> Any:
        import pyarrow as pa

        return pa.table(
            {
                first: pa.array(_dates(FIRST), pa.date32()),
                second: pa.array(_dates(SECOND), pa.date32()),
            }
        )

    def parquet_file(first: str, second: str) -> Any:
        import pyarrow.parquet as pq

        path = tmp_path / f"{first}-{second}.parquet"
        pq.write_table(arrow_table(first, second), path)
        return dp.profile(str(path))

    def arrow(first: str, second: str) -> Any:
        return dp.profile(arrow_table(first, second))

    def pandas(first: str, second: str) -> Any:
        import pandas as pd

        return dp.profile(pd.DataFrame({first: FIRST, second: SECOND}))

    def polars(first: str, second: str) -> Any:
        import polars as pl

        return dp.profile(pl.DataFrame({first: FIRST, second: SECOND}))

    routes: dict[str, Route] = {
        "auto": csv_file("auto"),
        "incremental": csv_file("incremental"),
        "columnar": csv_file("columnar"),
        "json": json_file,
        "jsonl": jsonl_file,
        "dict": lambda first, second: dp.profile({first: FIRST, second: SECOND}),
        "rows": lambda first, second: dp.profile(records(first, second)),
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


def test_names_that_merely_contain_a_role_word_are_not_paired(tmp_path: Path):
    wrong = []
    for label, route in _routes(tmp_path).items():
        for first, second in NOT_PAIRS:
            checked, violations = _ordering(route(first, second))
            if (checked, violations) != (0, 0):
                wrong.append(
                    f"[{label}] {first}/{second}: pairs_checked {checked}, violations {violations}"
                )
    assert not wrong, "\n".join(wrong)


def test_names_with_role_words_are_still_paired(tmp_path: Path):
    wrong = []
    for label, route in _routes(tmp_path).items():
        for first, second in PAIRS:
            checked, violations = _ordering(route(first, second))
            if (checked, violations) != (3, 3):
                wrong.append(
                    f"[{label}] {first}/{second}: pairs_checked {checked}, violations {violations}"
                )
    assert not wrong, "\n".join(wrong)
