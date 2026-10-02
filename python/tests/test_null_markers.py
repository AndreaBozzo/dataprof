"""Common missing-value markers count as nulls (#813).

Only empty, ``null`` and ``nan`` used to be nulls, so ``NA``, ``N/A``, ``n/a``,
``#N/A``, ``\\N`` and ``None`` were counted as values and completeness read 100%
on columns that were half missing. ``tests/null_markers.rs`` is the Rust twin.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

try:
    import dataprof as dp
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

#: Ten rows. ``na`` and ``mysql`` are missing on the even rows; ``txt`` holds
#: look-alikes that are values.
ROWS = [
    ("1", "a", "Na"),
    ("NA", "\\N", "none"),
    ("3", "b", "-"),
    ("N/A", "\\N", "?"),
    ("5", "c", "x"),
    ("#N/A", "\\N", "y"),
    ("7", "d", "n.d."),
    ("n/a", "\\N", "z"),
    ("9", "e", "NONE"),
    ("None", "\\N", "w"),
]
NAMES = ("na", "mysql", "txt")


def _columns() -> dict[str, list[str]]:
    return {name: [row[i] for row in ROWS] for i, name in enumerate(NAMES)}


def _records() -> list[dict[str, str]]:
    return [dict(zip(NAMES, row, strict=True)) for row in ROWS]


def _assert_markers_are_nulls(report: dp.ProfileReport, label: str) -> None:
    na = report["na"]
    assert na.null_count == 5, label
    assert na.data_type == "integer", label
    assert (na.min, na.max, na.mean) == (1.0, 9.0, 5.0), label
    assert report["mysql"].null_count == 5, label
    assert report["txt"].null_count == 0, label

    assert report.quality is not None, label
    completeness = report.quality.completeness
    assert completeness is not None, label
    assert completeness["complete_records_ratio"] == pytest.approx(50.0), label
    assert completeness["missing_values_ratio"] == pytest.approx(100 / 3), label
    semantics = report.metric_semantics
    assert semantics is not None, label
    assert semantics["null_tokens"] == "common_markers", label


@pytest.fixture
def csv_path(tmp_path: Path) -> Path:
    path = tmp_path / "markers.csv"
    lines = [",".join(NAMES), *(",".join(row) for row in ROWS)]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


@pytest.mark.parametrize("engine", ["auto", "incremental", "columnar"])
def test_every_engine_counts_the_markers_as_nulls(csv_path: Path, engine: str):
    _assert_markers_are_nulls(dp.profile(str(csv_path), engine=engine), engine)


@pytest.mark.parametrize("suffix", [".json", ".jsonl"])
def test_json_files(tmp_path: Path, suffix: str):
    path = tmp_path / f"markers{suffix}"
    if suffix == ".json":
        path.write_text(json.dumps(_records()), encoding="utf-8")
    else:
        path.write_text("\n".join(json.dumps(r) for r in _records()) + "\n", encoding="utf-8")
    _assert_markers_are_nulls(dp.profile(str(path)), suffix)


def test_in_memory_columns_and_records():
    _assert_markers_are_nulls(dp.profile(_columns()), "dict")
    _assert_markers_are_nulls(dp.profile(_records()), "records")


def test_pandas():
    pd = pytest.importorskip("pandas")
    _assert_markers_are_nulls(dp.profile(pd.DataFrame(_columns())), "pandas")


def test_polars():
    pl = pytest.importorskip("polars")
    _assert_markers_are_nulls(dp.profile(pl.DataFrame(_columns())), "polars")


def test_arrow_and_parquet(tmp_path: Path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    table = pa.table(_columns())
    _assert_markers_are_nulls(dp.profile(table), "arrow")
    path = tmp_path / "markers.parquet"
    pq.write_table(table, path)
    _assert_markers_are_nulls(dp.profile(str(path)), "parquet")
