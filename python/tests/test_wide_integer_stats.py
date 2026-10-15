"""The spread of 64-bit integers beyond 2^53 (#877).

Statistics used to be taken on ``float(value)``, where integers near 1.76e18
(epoch nanoseconds) sit 256 apart, so values a few units apart reported a
variance of 0.0. Spread and shape do not depend on a shift, so each wide column
has a twin holding the same values minus the first one: every input route has
to serialize the same variance, std_dev, skewness and kurtosis for both.

``tests/wide_integer_stats.rs`` is the Rust twin.
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

#: ``reading`` is signed (``int64``), ``counter`` lies beyond ``int64`` max
#: (``uint64``). Each ``_offset`` column is its twin minus the first value.
COLUMNS: dict[str, list[int]] = {
    "reading": [
        1759700000123456789,
        1759700000123456791,
        1759700000123456795,
        1759700000123456800,
    ],
    "reading_offset": [0, 2, 6, 11],
    "counter": [
        18446744073709551000,
        18446744073709551005,
        18446744073709551010,
        18446744073709551015,
    ],
    "counter_offset": [0, 5, 10, 15],
}
UNSIGNED = ("counter", "counter_offset")

#: (wide column, its twin, the twin's sample variance rounded as serialized).
PAIRS = [("reading", "reading_offset", 23.5833), ("counter", "counter_offset", 41.6667)]
SPREAD = ("variance", "std_dev", "skewness", "kurtosis")


def _assert_spread_kept(report: dp.ProfileReport, label: str) -> None:
    stats = {column["name"]: column["stats"] for column in report.to_dict()["columns"]}
    wrong = []
    for wide, twin, variance in PAIRS:
        if stats[twin]["variance"] != variance:
            wrong.append(f"{twin}.variance {stats[twin]['variance']}, exact {variance}")
        for statistic in SPREAD:
            got, expected = stats[wide].get(statistic), stats[twin].get(statistic)
            if got != expected:
                wrong.append(f"{wide}.{statistic} {got}, {twin} reports {expected}")
    assert not wrong, f"[{label}] {wrong}"


@pytest.fixture
def csv_path(tmp_path: Path) -> Path:
    path = tmp_path / "wide.csv"
    rows = zip(*COLUMNS.values(), strict=True)
    lines = [",".join(COLUMNS), *(",".join(map(str, row)) for row in rows)]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


@pytest.mark.parametrize("engine", ["auto", "incremental", "columnar"])
def test_every_engine_keeps_the_spread(csv_path: Path, engine: str):
    _assert_spread_kept(dp.profile(str(csv_path), engine=engine), engine)


@pytest.mark.parametrize("as_text", [False, True], ids=["ints", "strings"])
def test_python_columns(as_text: bool):
    columns: dict[str, list[Any]] = {
        name: [str(value) for value in values] if as_text else values
        for name, values in COLUMNS.items()
    }
    _assert_spread_kept(dp.profile(columns), "dict")


def _arrow_table(as_text: bool = False) -> Any:
    pa = pytest.importorskip("pyarrow")
    if as_text:
        return pa.table({name: [str(v) for v in values] for name, values in COLUMNS.items()})
    return pa.table(
        {
            name: pa.array(values, pa.uint64() if name in UNSIGNED else pa.int64())
            for name, values in COLUMNS.items()
        }
    )


@pytest.mark.parametrize("as_text", [False, True], ids=["integers", "strings"])
def test_pyarrow(as_text: bool):
    _assert_spread_kept(dp.profile(_arrow_table(as_text)), "pyarrow")


def test_pandas():
    pd = pytest.importorskip("pandas")
    frame = pd.DataFrame(
        {
            name: pd.Series(values, dtype="uint64" if name in UNSIGNED else "int64")
            for name, values in COLUMNS.items()
        }
    )
    _assert_spread_kept(dp.profile(frame), "pandas")


def test_polars():
    pl = pytest.importorskip("polars")
    frame = pl.DataFrame(
        {
            name: pl.Series(name, values, dtype=pl.UInt64 if name in UNSIGNED else pl.Int64)
            for name, values in COLUMNS.items()
        }
    )
    _assert_spread_kept(dp.profile(frame), "polars")


def test_parquet(tmp_path: Path):
    pq = pytest.importorskip("pyarrow.parquet")
    path = tmp_path / "wide.parquet"
    pq.write_table(_arrow_table(), path)
    _assert_spread_kept(dp.profile(str(path)), "parquet")


def test_jsonl(tmp_path: Path):
    path = tmp_path / "wide.jsonl"
    records = [dict(zip(COLUMNS, row, strict=True)) for row in zip(*COLUMNS.values(), strict=True)]
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")
    _assert_spread_kept(dp.profile(str(path)), "jsonl")
