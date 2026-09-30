"""Decimal columns are profiled at their value (#821).

A decimal's stored integer is its value times 10**scale; the Arrow/Parquet
path fed that integer to the statistics, so prices at two decimals came out a
hundred times too large, and ``decimal256`` values were never read at all. The
same values written to a CSV are the reference. ``tests/decimal_columns.rs`` is
the Rust twin.
"""

from __future__ import annotations

import decimal
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

VALUES = ["1.10", "2.25", "-3.00", "1234.56", "2.25"]
DECIMALS = [decimal.Decimal(v) for v in VALUES]


def _column(report: dp.ProfileReport) -> dict[str, Any]:
    return json.loads(report.to_json())["column_profiles"][0]


@pytest.fixture(scope="module")
def reference(tmp_path_factory) -> dict[str, Any]:
    path = tmp_path_factory.mktemp("decimal") / "amounts.csv"
    path.write_text("amount\n" + "\n".join(VALUES) + "\n", encoding="utf-8")
    return _column(dp.profile(str(path)))


def _assert_like(profiled: dict[str, Any], reference: dict[str, Any], label: str) -> None:
    for field in ("data_type", "unique_count", "invalid_count"):
        assert profiled[field] == reference[field], f"[{label}] {field}"
    for statistic in ("min", "max", "mean", "std_dev", "median"):
        assert (
            profiled["stats"]["Numeric"][statistic] == reference["stats"]["Numeric"][statistic]
        ), f"[{label}] {statistic}"


def test_pandas_decimal_objects(reference):
    pd = pytest.importorskip("pandas")
    _assert_like(_column(dp.profile(pd.DataFrame({"amount": DECIMALS}))), reference, "pandas")


def test_polars_decimal(reference):
    pl = pytest.importorskip("polars")
    frame = pl.DataFrame({"amount": pl.Series(DECIMALS, dtype=pl.Decimal(10, 2))})
    _assert_like(_column(dp.profile(frame)), reference, "polars")


@pytest.mark.parametrize("make", ["decimal128(10, 2)", "decimal128(20, 4)", "decimal256(40, 2)"])
def test_arrow_decimals(reference, make: str):
    pa = pytest.importorskip("pyarrow")
    kind, args = make.split("(")
    precision, scale = (int(x) for x in args.rstrip(")").split(","))
    table = pa.table({"amount": pa.array(DECIMALS, type=getattr(pa, kind)(precision, scale))})
    _assert_like(_column(dp.profile(table)), reference, make)


def test_parquet_file(reference, tmp_path: Path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    path = tmp_path / "amounts.parquet"
    pq.write_table(pa.table({"amount": pa.array(DECIMALS, type=pa.decimal128(10, 2))}), path)
    _assert_like(_column(dp.profile(str(path))), reference, "parquet")


def test_a_negative_scale_multiplies_the_stored_integer():
    pa = pytest.importorskip("pyarrow")
    # 12 stored at scale -2 is 1200.
    values = [decimal.Decimal("100"), decimal.Decimal("2200"), decimal.Decimal("-300")]
    table = pa.table({"v": pa.array(values, type=pa.decimal128(5, -2))})
    stats = _column(dp.profile(table))["stats"]["Numeric"]
    assert (stats["min"], stats["max"]) == (-300.0, 2200.0)
