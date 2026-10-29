"""A decimal's spread survives digits that ``f64`` does not keep (#1149).

``1000000000000000.01`` and ``1000000000000000.05`` are one ``f64``, so a column
of cents on 10**15 reported a variance, standard deviation, skewness and
kurtosis of 0.0 although every value differs, and 15 digits on 10**13 reported
a standard deviation off in its third digit. Such a literal is now held as a
scaled integer, and its deviations taken exactly, so each statistic is the one
of the exact values. Where a value is too wide to hold, as a ``decimal256`` of
76 digits is, a spread too small to stand above its rounding is absent rather
than 0.0.

A column typed as a decimal (Arrow, Parquet, pandas and polars decimals) is
held exactly from 11 significant digits. Text, ``Decimal`` objects in a dict or
records included, is held exactly from 18, the most a float's shortest form has
fewer than, so below that it reads as the float it may be (#783): the 15-digit
column is checked on typed routes only. ``tests/decimal_spread.rs`` is the Rust
twin.
"""

from __future__ import annotations

import io
import json
import math
from decimal import Decimal
from fractions import Fraction
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

CENTS = [1, 5, 2, 0, 9, 1, 5, 3]
#: name -> (values, Arrow type as (kind, precision, scale))
COLUMNS: dict[str, tuple[list[Decimal], tuple[str, int, int]]] = {
    "cents": (
        [Decimal(f"1000000000000000.{c:02}") for c in CENTS],
        ("decimal128", 18, 2),
    ),
    # An f64 keeps these apart, and rounds each by up to a tenth of a cent.
    "fifteen": (
        [Decimal(f"9999999999999.{c:02}") for c in CENTS],
        ("decimal128", 15, 2),
    ),
    "wide_cents": (
        [Decimal(f"123456789012345678901234567890123456.{c:02}") for c in CENTS],
        ("decimal128", 38, 2),
    ),
    # 10**75 plus a multiple of 100: beyond what a 128-bit integer holds.
    "unheld": (
        [Decimal(10**75 + c * 100) for c in CENTS],
        ("decimal256", 76, 0),
    ),
}
SPREAD = ("variance", "std_dev", "skewness", "kurtosis")
#: Held exactly only where the source types the column as a decimal.
TYPED_ONLY = ("fifteen",)


def _exact_spread(values: list[Decimal]) -> dict[str, float]:
    xs = [Fraction(v) for v in values]
    n = len(xs)
    mean = sum(xs, Fraction(0)) / n

    def moment(k: int) -> Fraction:
        total = Fraction(0)
        for x in xs:
            term = Fraction(1)
            for _ in range(k):
                term *= x - mean
            total += term
        return total

    variance = moment(2) / (n - 1)
    std_dev = math.sqrt(variance)
    skewness = n / ((n - 1) * (n - 2)) * float(moment(3)) / std_dev**3
    kurtosis = n * (n + 1) / ((n - 1) * (n - 2) * (n - 3)) * float(moment(4)) / std_dev**4 - 3 * (
        n - 1
    ) ** 2 / ((n - 2) * (n - 3))
    return {
        "variance": float(variance),
        "std_dev": std_dev,
        "skewness": skewness,
        "kurtosis": kurtosis,
    }


def _assert_exact(
    report: dp.ProfileReport, label: str, *, typed: bool, names: list[str] | None = None
) -> None:
    stats = {column["name"]: column["stats"] for column in report.to_dict()["columns"]}
    wrong = {}
    for name in names or list(COLUMNS):
        if name in TYPED_ONLY and not typed:
            continue
        column = stats.get(name) or {}
        if name == "unheld":
            for key in (*SPREAD, "coefficient_of_variation"):
                if column.get(key) is not None:
                    wrong[f"{name} {key}"] = column.get(key)
            continue
        for key, expected in _exact_spread(COLUMNS[name][0]).items():
            got = column.get(key)
            # Serialized at four decimals.
            if got is None or abs(got - expected) > 5e-5 + 1e-9 * abs(expected):
                wrong[f"{name} {key}"] = (got, expected)
    assert not wrong, (label, wrong)


def _csv_text() -> str:
    rows = zip(*(values for values, _ in COLUMNS.values()), strict=True)
    lines = [",".join(COLUMNS), *(",".join(str(v) for v in row) for row in rows)]
    return "\n".join(lines) + "\n"


@pytest.fixture
def csv_path(tmp_path: Path) -> Path:
    path = tmp_path / "spread.csv"
    path.write_text(_csv_text(), encoding="utf-8")
    return path


@pytest.mark.parametrize("engine", ["auto", "incremental", "columnar"])
def test_every_engine(csv_path: Path, engine: str):
    _assert_exact(dp.profile(str(csv_path), engine=engine), engine, typed=False)


def test_csv_bytes():
    _assert_exact(dp.profile(_csv_text().encode(), format="csv"), "csv bytes", typed=False)


def test_jsonl_strings(tmp_path: Path):
    # A JSON number is an f64 before it reaches the profiler; a string is not.
    path = tmp_path / "spread.jsonl"
    rows = zip(*(values for values, _ in COLUMNS.values()), strict=True)
    records = [{name: str(v) for name, v in zip(COLUMNS, row, strict=True)} for row in rows]
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")
    _assert_exact(dp.profile(str(path)), "jsonl", typed=False)


def _arrow_table() -> Any:
    pa = pytest.importorskip("pyarrow")
    return pa.table(
        {
            name: pa.array(values, getattr(pa, kind)(precision, scale))
            for name, (values, (kind, precision, scale)) in COLUMNS.items()
        }
    )


def test_arrow_table():
    _assert_exact(dp.profile(_arrow_table()), "arrow table", typed=True)


def test_arrow_stream_of_two_batches():
    pa = pytest.importorskip("pyarrow")
    batches = _arrow_table().to_batches(max_chunksize=len(CENTS) // 2)
    assert len(batches) == 2
    reader = pa.RecordBatchReader.from_batches(batches[0].schema, batches)
    _assert_exact(dp.profile(reader), "arrow stream", typed=True)


def test_parquet_file_and_bytes(tmp_path: Path):
    pq = pytest.importorskip("pyarrow.parquet")
    path = tmp_path / "spread.parquet"
    pq.write_table(_arrow_table(), path)
    _assert_exact(dp.profile(str(path)), "parquet file", typed=True)
    buffer = io.BytesIO()
    pq.write_table(_arrow_table(), buffer)
    _assert_exact(dp.profile(buffer.getvalue(), format="parquet"), "parquet bytes", typed=True)


def test_decimal_records_and_columns():
    columns = {name: values for name, (values, _) in COLUMNS.items()}
    _assert_exact(dp.profile(columns), "dict of Decimal", typed=False)
    rows = zip(*columns.values(), strict=True)
    records = [dict(zip(columns, row, strict=True)) for row in rows]
    _assert_exact(dp.profile(records), "Decimal records", typed=False)


def test_pandas_decimal_objects():
    pd = pytest.importorskip("pandas")
    frame = pd.DataFrame({name: values for name, (values, _) in COLUMNS.items()})
    _assert_exact(dp.profile(frame), "pandas", typed=True)


def test_polars_decimal():
    pl = pytest.importorskip("polars")
    # Polars decimals hold at most 38 digits.
    names = ["cents", "fifteen", "wide_cents"]
    frame = pl.DataFrame(
        {
            name: pl.Series(COLUMNS[name][0], dtype=pl.Decimal(*COLUMNS[name][1][1:]))
            for name in names
        }
    )
    _assert_exact(dp.profile(frame), "polars", typed=True, names=names)
