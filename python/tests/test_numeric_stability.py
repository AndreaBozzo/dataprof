"""Numeric aggregates must be numerically stable on every route (#670, #671).

Two failures found in a 221-profile dogfooding matrix, both of which survive
report rounding and read as ordinary numbers:

* a variance computed as ``sum_squares - n * mean**2`` cancels away the spread
  of a column sitting on a large offset — four consecutive integers near 1e9
  came back with a variance of exactly 0.0, describing a varying column as
  constant, and near 1e8 with a variance 60% too large;
* a naive running sum drops a small contribution between large values that
  cancel (the mean of ``[1e16, 1.0, -1e16]`` came back 0.0 instead of 1/3) and
  overflows where the mean itself is representable.

Six routes disagreed with the batch reference, so every case runs over all of
them and over the serialized report as well as the attribute.
"""

from __future__ import annotations

import asyncio
import json
import math
import random
import statistics
import sys
from fractions import Fraction

import pytest

try:
    import dataprof as dp
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop --features python",
        allow_module_level=True,
    )

from dataprof.asyncio import _HAS_ASYNC

ROUTES = ("dict", "csv", "columnar", "json", "jsonl", "bytes", "async", "arrow", "parquet")


def _profile(route, values, tmp_path):
    """Profile a one-column dataset of ``values`` through one input route."""
    text = "\n".join(repr(value) for value in values)

    if route == "dict":
        return dp.profile({"x": list(values)})
    if route in ("csv", "columnar"):
        path = tmp_path / "values.csv"
        path.write_text(f"x\n{text}\n", encoding="utf-8")
        return dp.profile(path, engine="columnar" if route == "columnar" else "incremental")
    if route == "json":
        path = tmp_path / "values.json"
        path.write_text(json.dumps([{"x": value} for value in values]), encoding="utf-8")
        return dp.profile(path)
    if route == "jsonl":
        path = tmp_path / "values.jsonl"
        path.write_text(
            "\n".join(json.dumps({"x": value}) for value in values) + "\n", encoding="utf-8"
        )
        return dp.profile(path)
    if route == "bytes":
        return dp.profile(f"x\n{text}\n".encode(), format="csv")
    if route == "async":
        if not _HAS_ASYNC:
            pytest.skip(
                "async streaming not compiled: build with --features "
                "'python,python-async,async-streaming'"
            )
        return asyncio.run(dp.asyncio.profile_bytes(f"x\n{text}\n".encode(), format="csv"))

    pa = pytest.importorskip("pyarrow")
    table = pa.table({"x": [float(value) for value in values]})
    if route == "arrow":
        return dp.profile(table)
    pq = pytest.importorskip("pyarrow.parquet")
    path = tmp_path / "values.parquet"
    pq.write_table(table, path)
    return dp.profile(path)


def _stats(report):
    """The column as an attribute and as the report serializes it."""
    return report["x"], report.to_dict()["columns"][0]["stats"]


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("base", [1e6, 1e8, 1e9, 1e12])
def test_variance_survives_a_large_offset(tmp_path, route, base):
    values = [base + offset for offset in range(4)]
    expected = statistics.variance(values)
    assert expected == pytest.approx(5 / 3)

    column, serialized = _stats(_profile(route, values, tmp_path))

    # Equality, not a tolerance: a stable accumulation of four values reaches
    # the correctly rounded answer, and every route here does.
    assert column.variance == expected
    assert column.std_dev == expected**0.5
    assert column.mean == base + 1.5
    # Rounding does not rescue this: 0.0 and 2.6667 round to themselves.
    assert serialized["variance"] == 1.6667
    assert serialized["std_dev"] == round(expected**0.5, 4)


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("offset", [1e12, 1e14])
def test_spread_on_a_large_offset_matches_the_exact_value(tmp_path, route, offset):
    """Welford on raw values near 1e12 was 2e-8 relative off (#783).

    Enough to move the serialized fourth decimal, and different routes rounded
    differently, so the same data serialized two ways.
    """
    rng = random.Random(783)
    values = [offset + rng.uniform(-1000, 1000) for _ in range(20_000)]
    exact = [Fraction(value) for value in values]
    mean = sum(exact) / len(exact)
    variance = float(sum((value - mean) ** 2 for value in exact) / (len(exact) - 1))
    std_dev = math.sqrt(variance)

    column, serialized = _stats(_profile(route, values, tmp_path))

    assert column.variance == pytest.approx(variance, rel=1e-12)
    assert column.std_dev == pytest.approx(std_dev, rel=1e-12)
    assert serialized["std_dev"] == round(std_dev, 4)


@pytest.mark.parametrize("route", ROUTES)
def test_a_constant_column_still_reports_no_spread(tmp_path, route):
    """The half a stability fix can get wrong: inventing spread out of noise."""
    column, serialized = _stats(_profile(route, [1e9] * 4, tmp_path))

    assert column.variance == 0.0
    assert column.std_dev == 0.0
    assert column.mean == 1e9
    assert serialized["variance"] == 0.0
    assert serialized["std_dev"] == 0.0


@pytest.mark.parametrize("route", ROUTES)
def test_overflowing_variance_is_absent_and_survives_save_load(tmp_path, route):
    values = [1e308, -1e308, 5.0, 0.5, 5.0, 7.0]
    report = _profile(route, values, tmp_path)
    column, serialized = _stats(report)

    assert column.variance is None
    assert column.std_dev is None
    assert column.coefficient_of_variation is None
    assert column.skewness is None
    assert column.kurtosis is None
    assert serialized["variance"] is None
    assert serialized["std_dev"] is None
    assert serialized["mean"] == 2.9167

    saved = tmp_path / f"overflow-{route}.json"
    report.save(saved)
    loaded = dp.ProfileReport.load(saved)
    assert loaded["x"].variance is None
    assert loaded["x"].std_dev is None
    assert loaded.to_dict()["columns"][0]["stats"] == serialized


@pytest.mark.parametrize("route", ROUTES)
def test_an_overflowing_pair_is_not_a_constant_column(tmp_path, route):
    """Two values whose difference overflows reported a variance of 0.0."""
    big = 0.9 * sys.float_info.max
    column, serialized = _stats(_profile(route, [big, -big], tmp_path))

    assert column.variance is None
    assert column.std_dev is None
    assert serialized["variance"] is None


@pytest.mark.parametrize("route", ROUTES)
def test_overflowing_iqr_is_absent_and_survives_save_load(tmp_path, route):
    report = _profile(route, [-1e308, 1e308, -1e308, 1e308], tmp_path)
    column, serialized = _stats(report)

    assert column.quartiles == {"q1": -1e308, "q2": 0.0, "q3": 1e308, "iqr": None}
    # Fences beyond the f64 range leave no finite value outside them.
    assert column.outlier_count == 0
    assert serialized["quartiles"]["iqr"] is None
    assert "iqr" in serialized["quartiles"]
    assert report.describe()["x"]["75%"] == 1e308

    saved = tmp_path / f"iqr-{route}.json"
    report.save(saved)
    loaded = dp.ProfileReport.load(saved)
    assert loaded["x"].quartiles == column.quartiles
    assert loaded.to_dict()["columns"][0]["stats"] == serialized


@pytest.mark.parametrize("field", ["std_dev", "variance", "iqr"])
def test_a_missing_spread_key_is_malformed_not_null(tmp_path, field):
    # null is a spread that overflowed f64; a missing key is a broken document.
    report = _profile("csv", [-1e308, 1e308, -1e308, 1e308], tmp_path)
    saved = tmp_path / "report.json"
    report.save(saved)
    document = json.loads(saved.read_text(encoding="utf-8"))
    stats = document["column_profiles"][0]["stats"]["Numeric"]
    del (stats["quartiles"] if field == "iqr" else stats)[field]
    saved.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ValueError, match=f"missing field `{field}`"):
        dp.ProfileReport.load(saved)


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize(
    "values",
    [
        [1e16, 1.0, -1e16],
        [1e16, -1e16, 1.0],
        [-1e16, 1.0, 1e16],
    ],
)
def test_mean_survives_cancelling_values(tmp_path, route, values):
    """Permutations matter: which value is dropped depends on the order."""
    expected = statistics.mean(values)
    assert expected == pytest.approx(1 / 3)

    column, serialized = _stats(_profile(route, values, tmp_path))

    assert column.mean == expected
    assert serialized["mean"] == 0.3333


@pytest.mark.parametrize("route", ROUTES)
def test_mean_stays_finite_when_the_naive_sum_overflows(tmp_path, route):
    """The mean of two 1e308 values is representable; their sum is not."""
    column, serialized = _stats(_profile(route, [1e308, 1e308], tmp_path))

    assert column.mean == 1e308
    # An infinite mean did not serialize at all, so the field went missing.
    assert serialized["mean"] == 1e308


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("repeat", [1, 25], ids=["four-values", "past-simd-threshold"])
def test_mean_of_an_overflowing_sum_that_cancels(tmp_path, route, repeat):
    """The sum overflows at the second value; the mean is 0.0 (#801).

    Welford's running mean, the old fallback, turned NaN here. NaN serialized
    as null and ``ProfileReport.load`` then rejected the report.
    """
    values = [1e308, 1e308, -1e308, -1e308] * repeat
    report = _profile(route, values, tmp_path)
    column, serialized = _stats(report)

    assert column.mean == 0.0
    assert serialized["mean"] == 0.0
    # The spread still exceeds the f64 range (#784).
    assert column.variance is None
    assert column.std_dev is None

    saved = tmp_path / f"mean-{route}.json"
    report.save(saved)
    loaded = dp.ProfileReport.load(saved)
    assert loaded["x"].mean == 0.0
    assert loaded.to_dict()["columns"][0]["stats"] == serialized


@pytest.mark.parametrize("route", ROUTES)
def test_stability_holds_past_the_simd_threshold(tmp_path, route):
    """Long enough to reach the four-lane accumulation, on a large offset."""
    values = [1e9 + (index % 4) for index in range(1000)]
    expected = statistics.variance(values)

    column, serialized = _stats(_profile(route, values, tmp_path))

    # Relative here, not exact: over a thousand values a running accumulation
    # drifts about 1e-10 relative at this offset. That is still five orders
    # tighter than the failure — the columnar engine reported 4985.7 for this
    # column, and the batch path 0.0 at a slightly larger offset.
    assert column.variance == pytest.approx(expected, rel=1e-9)
    assert column.mean == pytest.approx(1e9 + 1.5, rel=1e-15)
    assert serialized["variance"] == pytest.approx(round(expected, 4), abs=1e-4)
