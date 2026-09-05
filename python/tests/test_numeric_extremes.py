"""Numeric distributions must retain actual values at floating-point extremes."""

from __future__ import annotations

import json

import dataprof
import pytest


@pytest.mark.parametrize("route", ["dict", "csv", "columnar", "json", "arrow", "parquet"])
@pytest.mark.parametrize(
    ("values", "field", "expected"),
    [
        ([1e-12, 2e-12, 3e-12, 4e-12], "mode", None),
        ([1e-12, 1e-12, 2e-12], "mode", 1e-12),
        ([-0.0, 0.0, 1.0, 1.0], "mode", 0.0),
        ([1e308, 1e308], "median", 1e308),
        ([-1e308, 1e308], "median", 0.0),
    ],
)
def test_extreme_distribution_values(tmp_path, route, values, field, expected):
    source = {"x": values}
    engine = "auto"
    if route in ("csv", "columnar"):
        source = tmp_path / "values.csv"
        source.write_text("x\n" + "\n".join(map(str, values)), encoding="utf-8")
        engine = "columnar" if route == "columnar" else "incremental"
    elif route == "json":
        source = tmp_path / "values.json"
        source.write_text(json.dumps([{"x": v} for v in values]), encoding="utf-8")
    elif route in ("arrow", "parquet"):
        pa = pytest.importorskip("pyarrow")
        source = pa.table(source)
        if route == "parquet":
            pq = pytest.importorskip("pyarrow.parquet")
            path = tmp_path / "values.parquet"
            pq.write_table(source, path)
            source = path

    column = dataprof.profile(source, engine=engine)["x"]
    # Exact equality matters: an absolute tolerance would accept a fabricated
    # zero as the mode of a column containing only tiny nonzero numbers.
    assert getattr(column, field) == expected


@pytest.mark.parametrize("value", [1e308, -1e308])
def test_large_finite_values_survive_native_serialization(value):
    report = dataprof.profile({"x": [value]})
    native_report = getattr(report, "_report")
    native = json.loads(native_report.to_json())["column_profiles"][0]["stats"]["Numeric"]
    public = report.to_dict()["columns"][0]["stats"]
    for field in ("min", "max", "mean", "median"):
        assert native[field] == public[field] == value
