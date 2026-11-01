"""Exact order statistics of integer and decimal columns (#1038).

``wide`` holds epoch nanoseconds a few units apart and one small value, which
past 2^53 round as floats; ``ints`` repeats small integers; ``floats`` is a
float column and has no ``exact``. ``cents`` is a decimal column, which the
typed routes hand over as a decimal: its values have more digits than a float
keeps, and ``12.30`` and ``12.3`` are one value. ``ColumnProfile.exact`` reads
an integral value of an integer column as ``int`` and every other value as
``Decimal``; ``to_dict()`` and ``to_json()`` carry canonical decimal strings.

``tests/exact_order_statistics.rs`` is the Rust twin.
"""

from __future__ import annotations

import json
from decimal import Decimal
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

WIDE = [
    1759700000123456789,
    1759700000123456791,
    1759700000123456795,
    1759700000123456800,
    3,
]
INTS = [4, 1, 2, 2, 9]
FLOATS = [0.5, 1.5, 2.5, 3.5, 4.5]
CENTS = [
    Decimal("99999999999999999999.99"),
    Decimal("99999999999999999999.98"),
    Decimal("0.01"),
    Decimal("12.30"),
    Decimal("12.3"),
]

WIDE_EXACT = {
    "min": 3,
    "max": 1759700000123456800,
    "median": 1759700000123456791,
    "q1": 1759700000123456789,
    "q2": 1759700000123456791,
    "q3": 1759700000123456795,
}
INTS_EXACT = {"min": 1, "max": 9, "median": 2, "q1": 2, "q2": 2, "q3": 4, "mode": 2}
CENTS_EXACT = {
    "min": Decimal("0.01"),
    "max": Decimal("99999999999999999999.99"),
    "median": Decimal("12.3"),
    "q1": Decimal("12.3"),
    "q2": Decimal("12.3"),
    "q3": Decimal("99999999999999999999.98"),
    "mode": Decimal("12.3"),
}


def _exact(report: dp.ProfileReport) -> dict[str, Any]:
    return {column.name: column.exact for column in report.column_profiles.values()}


def _assert_integers(report: dp.ProfileReport, label: str) -> None:
    exact = _exact(report)
    assert exact["wide"] == WIDE_EXACT, label
    assert exact["ints"] == INTS_EXACT, label
    assert exact["floats"] is None, label
    # Integral values of an integer column are ints, not Decimals.
    assert all(type(value) is int for value in exact["wide"].values()), label


@pytest.fixture
def csv_path(tmp_path: Path) -> Path:
    path = tmp_path / "exact.csv"
    rows = zip(WIDE, INTS, FLOATS, strict=True)
    path.write_text(
        "wide,ints,floats\n" + "".join(f"{w},{i},{f}\n" for w, i, f in rows),
        encoding="utf-8",
    )
    return path


@pytest.mark.parametrize("engine", ["auto", "incremental", "columnar"])
def test_every_engine(csv_path: Path, engine: str):
    _assert_integers(dp.profile(str(csv_path), engine=engine), engine)


def test_python_columns():
    _assert_integers(dp.profile({"wide": WIDE, "ints": INTS, "floats": FLOATS}), "dict")


def _arrow_table() -> Any:
    pa = pytest.importorskip("pyarrow")
    return pa.table(
        {
            "wide": pa.array(WIDE, pa.int64()),
            "ints": pa.array(INTS, pa.int64()),
            "floats": pa.array(FLOATS, pa.float64()),
            "cents": pa.array(CENTS, pa.decimal128(22, 2)),
        }
    )


def test_pyarrow_and_decimals():
    report = dp.profile(_arrow_table())
    _assert_integers(report, "pyarrow")
    assert _exact(report)["cents"] == CENTS_EXACT


def test_pandas():
    pd = pytest.importorskip("pandas")
    _assert_integers(
        dp.profile(pd.DataFrame({"wide": WIDE, "ints": INTS, "floats": FLOATS})), "pandas"
    )


def test_polars():
    pl = pytest.importorskip("polars")
    _assert_integers(
        dp.profile(pl.DataFrame({"wide": WIDE, "ints": INTS, "floats": FLOATS})), "polars"
    )


def test_parquet(tmp_path: Path):
    pq = pytest.importorskip("pyarrow.parquet")
    path = tmp_path / "exact.parquet"
    pq.write_table(_arrow_table(), path)
    report = dp.profile(str(path))
    _assert_integers(report, "parquet")
    assert _exact(report)["cents"] == CENTS_EXACT


def test_extremes_cover_values_past_the_sample(tmp_path: Path):
    """``min`` and ``max`` cover every value, where the sample may miss them."""
    rows = 60_000
    values = [1759700000123456789 + row % 1000 for row in range(rows)]
    values[31_337] = 1759700000000000001
    values[47_111] = 1759700000999999999
    path = tmp_path / "sampled.csv"
    path.write_text("v\n" + "".join(f"{v}\n" for v in values), encoding="utf-8")
    routes = {
        "dict": lambda: dp.profile({"v": values}),
        "bytes": lambda: dp.profile(path.read_bytes(), format="csv"),
        **{
            engine: (lambda engine=engine: dp.profile(str(path), engine=engine))
            for engine in ("auto", "incremental", "columnar")
        },
    }
    wrong = {}
    for label, run in routes.items():
        exact = run()["v"].exact
        if (exact["min"], exact["max"]) != (1759700000000000001, 1759700000999999999):
            wrong[label] = (exact["min"], exact["max"])
    assert not wrong, wrong


def test_documents_carry_canonical_strings(tmp_path: Path):
    report = dp.profile(_arrow_table())
    summary = {c["name"]: c["stats"].get("exact") for c in report.to_dict()["columns"]}
    assert summary["wide"]["max"] == "1759700000123456800"
    assert summary["cents"]["max"] == "99999999999999999999.99"
    assert summary["floats"] is None
    canonical = json.loads(report.to_json())
    stats = {c["name"]: c["stats"]["Numeric"] for c in canonical["column_profiles"]}
    assert stats["cents"]["exact"] == summary["cents"]
    assert "exact" not in stats["floats"]

    # A saved report reads back the same typed values, from either document.
    path = tmp_path / "report.json"
    report.save(path)
    assert _exact(dp.ProfileReport.load(path)) == _exact(report)
    restored = dp.ProfileReport.from_dict(report.to_dict())
    assert _exact(restored) == _exact(report)
    # A restored report writes the strings back out unchanged.
    assert restored.to_dict()["columns"] == report.to_dict()["columns"]
