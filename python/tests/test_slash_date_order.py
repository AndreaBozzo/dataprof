"""A column's slash dates are read in one day/month order (#811).

Each value used to be parsed on its own, so a US export read ``12/31/2024``
month-first and ``01/02/2024`` day-first, and half its dates were wrong by
months. ``tests/slash_date_order.rs`` is the Rust twin.
"""

from __future__ import annotations

import json
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

#: A US export: every value month-first, every row running forwards.
ROWS = [
    ("12/31/2024", "01/13/2025"),
    ("01/02/2024", "02/01/2024"),
    ("03/04/2024", "03/05/2024"),
    ("12/30/2024", "12/31/2024"),
    ("05/06/2024", "06/05/2024"),
    ("11/29/2024", "11/30/2024"),
]


def _columns() -> dict[str, list[str]]:
    return {"start_date": [r[0] for r in ROWS], "end_date": [r[1] for r in ROWS]}


def _assert_month_first(report: dp.ProfileReport, label: str) -> None:
    document: dict[str, Any] = json.loads(report.to_json())
    start = next(c for c in document["column_profiles"] if c["name"] == "start_date")
    stats = start["stats"]["DateTime"]
    assert stats["slash_date_order"] == "month_first", label
    assert stats["min_datetime"] == "2024-01-02", label
    assert stats["max_datetime"] == "2024-12-31", label
    assert {int(k): v for k, v in stats["month_distribution"].items()} == {
        1: 1,
        3: 1,
        5: 1,
        11: 1,
        12: 2,
    }, label

    assert report.quality is not None, label
    timeliness = report.quality.timeliness
    assert timeliness is not None, label
    assert timeliness["temporal_pairs_checked"] == 6, label
    assert timeliness["temporal_violations"] == 0, label


@pytest.fixture
def csv_path(tmp_path: Path) -> Path:
    path = tmp_path / "us.csv"
    lines = ["start_date,end_date", *(f"{s},{e}" for s, e in ROWS)]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


@pytest.mark.parametrize("engine", ["auto", "incremental", "columnar"])
def test_every_engine_reads_a_us_column_month_first(csv_path: Path, engine: str):
    _assert_month_first(dp.profile(str(csv_path), engine=engine), engine)


def test_pandas():
    pd = pytest.importorskip("pandas")
    _assert_month_first(dp.profile(pd.DataFrame(_columns())), "pandas")


def test_polars():
    pl = pytest.importorskip("polars")
    _assert_month_first(dp.profile(pl.DataFrame(_columns())), "polars")


def test_pyarrow():
    pa = pytest.importorskip("pyarrow")
    _assert_month_first(dp.profile(pa.table(_columns())), "pyarrow")


def test_parquet(tmp_path: Path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    path = tmp_path / "us.parquet"
    pq.write_table(pa.table(_columns()), path)
    _assert_month_first(dp.profile(str(path)), "parquet")


def test_jsonl(tmp_path: Path):
    path = tmp_path / "us.jsonl"
    records = [{"start_date": s, "end_date": e} for s, e in ROWS]
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")
    _assert_month_first(dp.profile(str(path)), "jsonl")


def test_the_order_survives_the_document_round_trip(csv_path: Path):
    report = dp.profile(str(csv_path))
    restored = dp.ProfileReport.from_json(report.to_json())
    _assert_month_first(restored, "restored")


@pytest.mark.parametrize(
    ("values", "order"),
    [
        (["31/12/2024", "01/02/2024"], "day_first"),
        (["01/02/2024", "03/04/2024"], "assumed_day_first"),
        (["12/31/2024", "31/12/2024", "01/02/2024"], "mixed"),
    ],
)
def test_the_recorded_order_says_how_it_was_decided(tmp_path: Path, values: list[str], order: str):
    path = tmp_path / "when.csv"
    path.write_text("when\n" + "\n".join(values) + "\n", encoding="utf-8")
    document = json.loads(dp.profile(str(path)).to_json())
    assert document["column_profiles"][0]["stats"]["DateTime"]["slash_date_order"] == order
