"""A variance that fits in ``f64`` where its sum of squares does not (#805).

``fits`` holds ``1e154`` and ``-1e154`` in alternating blocks of four, 52 each:
its sample variance, 1.04e310 / 103, is about 1.01e308, but the running sum of
squared deviations overflows, and ``variance`` and ``std_dev`` used to be
``None``. The blocks flip every four-lane SIMD lane too. ``exceeds`` is
ten times the spread, a variance no ``f64`` holds, and still reports ``None``.

``tests/large_spread_variance.rs`` is the Rust twin.
"""

from __future__ import annotations

import json
import math
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

COLUMNS: dict[str, list[float]] = {
    "fits": ([1e154] * 4 + [-1e154] * 4) * 13,
    "exceeds": ([1e155] * 4 + [-1e155] * 4) * 13,
}
#: 1.04e310 / 103 by exact arithmetic.
FITS_VARIANCE = 1.0097087378640777e308


def _assert_spread(report: dp.ProfileReport, label: str) -> None:
    stats = {column["name"]: column["stats"] for column in report.to_dict()["columns"]}
    fits = stats["fits"]
    assert fits["variance"] is not None, label
    assert math.isclose(fits["variance"], FITS_VARIANCE, rel_tol=1e-9), (label, fits)
    assert math.isclose(fits["std_dev"], math.sqrt(FITS_VARIANCE), rel_tol=1e-9), (label, fits)
    exceeds = stats["exceeds"]
    assert exceeds["variance"] is None and exceeds["std_dev"] is None, (label, exceeds)


@pytest.fixture
def csv_path(tmp_path: Path) -> Path:
    path = tmp_path / "spread.csv"
    rows = zip(*COLUMNS.values(), strict=True)
    lines = [",".join(COLUMNS), *(",".join(repr(v) for v in row) for row in rows)]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


@pytest.mark.parametrize("engine", ["auto", "incremental", "columnar"])
def test_every_engine(csv_path: Path, engine: str):
    _assert_spread(dp.profile(str(csv_path), engine=engine), engine)


def test_python_columns():
    _assert_spread(dp.profile(COLUMNS), "dict")


def _arrow_table() -> Any:
    pa = pytest.importorskip("pyarrow")
    return pa.table({name: pa.array(values, pa.float64()) for name, values in COLUMNS.items()})


def test_pyarrow():
    _assert_spread(dp.profile(_arrow_table()), "pyarrow")


def test_pandas():
    pd = pytest.importorskip("pandas")
    _assert_spread(dp.profile(pd.DataFrame(COLUMNS)), "pandas")


def test_polars():
    pl = pytest.importorskip("polars")
    _assert_spread(dp.profile(pl.DataFrame(COLUMNS)), "polars")


def test_parquet(tmp_path: Path):
    pq = pytest.importorskip("pyarrow.parquet")
    path = tmp_path / "spread.parquet"
    pq.write_table(_arrow_table(), path)
    _assert_spread(dp.profile(str(path)), "parquet")


def test_jsonl(tmp_path: Path):
    path = tmp_path / "spread.jsonl"
    records = [dict(zip(COLUMNS, row, strict=True)) for row in zip(*COLUMNS.values(), strict=True)]
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")
    _assert_spread(dp.profile(str(path)), "jsonl")
