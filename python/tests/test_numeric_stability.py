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
import statistics

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
def test_a_constant_column_still_reports_no_spread(tmp_path, route):
    """The half a stability fix can get wrong: inventing spread out of noise."""
    column, serialized = _stats(_profile(route, [1e9] * 4, tmp_path))

    assert column.variance == 0.0
    assert column.std_dev == 0.0
    assert column.mean == 1e9
    assert serialized["variance"] == 0.0
    assert serialized["std_dev"] == 0.0


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
