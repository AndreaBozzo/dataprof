"""A spread too small to square in ``f64`` keeps its scale-free statistics (#1088).

Values near 1e-200 have squared deviations that underflow to zero. The variance
read 0.0, and the coefficient of variation and the kurtosis then described a
constant column: 0.0 at 1e-200, and a drifting kurtosis already at 1e-160. They
do not depend on scale, so every column below must report the coefficient of
variation, skewness and kurtosis of the same values at unit scale, from 1e-300
to 1e100. A variance below ``f64``'s range is reported as absent, as one above
it is (#805). From 1e160 the spread itself overflows, and every statistic
derived from it is absent rather than wrong.

``cycled`` repeats its pattern every row, so each four-lane SIMD lane is
constant and the spread is all in the merge of the lanes; ``blocked`` repeats
it in blocks of four, so each lane varies. ``tests/small_spread_variance.rs`` is
the Rust twin.
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

PATTERN = [1.0, 2.0, 4.0, 9.0]
ROWS = 104
SHAPES = {
    "cycled": [PATTERN[row % 4] for row in range(ROWS)],
    "blocked": [PATTERN[(row // 4) % 4] for row in range(ROWS)],
}
#: Scale -> what the variance, about the scale squared, does in f64: "fits",
#: "underflows" (reported absent, the rest kept) or "overflows" (all absent).
SCALES = {
    "1": "fits",
    "1e-160": "fits",
    "1e-200": "underflows",
    "1e-300": "underflows",
    "1e100": "fits",
    "1e160": "overflows",
    "1e300": "overflows",
}
COLUMNS: dict[str, list[float]] = {
    f"{shape}_{scale}": [value * float(scale) for value in values]
    for shape, values in SHAPES.items()
    for scale in SCALES
}
SCALE_FREE = ("coefficient_of_variation", "skewness", "kurtosis")


def _assert_scale_free(report: dp.ProfileReport, label: str) -> None:
    stats = {column["name"]: column["stats"] for column in report.to_dict()["columns"]}
    wrong = {}
    for shape in SHAPES:
        unit = stats[f"{shape}_1"]
        for scale, fate in SCALES.items():
            column = stats[f"{shape}_{scale}"]
            for key in SCALE_FREE:
                expected = None if fate == "overflows" else unit[key]
                if column.get(key) != expected or (expected is None and fate != "overflows"):
                    wrong[f"{shape}_{scale} {key}"] = (column.get(key), expected)
            if (column["variance"] is None) != (fate != "fits"):
                wrong[f"{shape}_{scale} variance"] = column["variance"]
            if (column["std_dev"] is None) != (fate == "overflows"):
                wrong[f"{shape}_{scale} std_dev"] = column["std_dev"]
    assert not wrong, (label, wrong)


@pytest.fixture
def csv_path(tmp_path: Path) -> Path:
    path = tmp_path / "spread.csv"
    rows = zip(*COLUMNS.values(), strict=True)
    lines = [",".join(COLUMNS), *(",".join(repr(v) for v in row) for row in rows)]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


@pytest.mark.parametrize("engine", ["auto", "incremental", "columnar"])
def test_every_engine(csv_path: Path, engine: str):
    _assert_scale_free(dp.profile(str(csv_path), engine=engine), engine)


def test_python_columns():
    _assert_scale_free(dp.profile(COLUMNS), "dict")


def _arrow_table() -> Any:
    pa = pytest.importorskip("pyarrow")
    return pa.table({name: pa.array(values, pa.float64()) for name, values in COLUMNS.items()})


def test_pyarrow():
    _assert_scale_free(dp.profile(_arrow_table()), "pyarrow")


def test_pandas():
    pd = pytest.importorskip("pandas")
    _assert_scale_free(dp.profile(pd.DataFrame(COLUMNS)), "pandas")


def test_polars():
    pl = pytest.importorskip("polars")
    _assert_scale_free(dp.profile(pl.DataFrame(COLUMNS)), "polars")


def test_parquet(tmp_path: Path):
    pq = pytest.importorskip("pyarrow.parquet")
    path = tmp_path / "spread.parquet"
    pq.write_table(_arrow_table(), path)
    _assert_scale_free(dp.profile(str(path)), "parquet")


def test_jsonl(tmp_path: Path):
    path = tmp_path / "spread.jsonl"
    records = [dict(zip(COLUMNS, row, strict=True)) for row in zip(*COLUMNS.values(), strict=True)]
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")
    _assert_scale_free(dp.profile(str(path)), "jsonl")
