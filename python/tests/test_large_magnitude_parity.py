"""Moment statistics at large magnitudes serialize alike on every route (#1178).

``tests/fixtures/large_magnitude_stats.csv`` holds 104 floats near 1e20. The
routes accumulate them in different orders, so the raw ``std_dev`` and
``variance`` differ in their last bits, and 4 decimal places stopped absorbing
that above about 1e11: the file routes serialized ``2.85136062083506e19`` and
the bytes and dict routes ``2.8513606208350605e19``. Moment statistics now
keep at most 12 significant digits, and every route must serialize the same
numbers.

``tests/large_magnitude_parity.rs`` is the Rust twin.
"""

from __future__ import annotations

import importlib.util
from collections.abc import Callable
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

FIXTURE = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "large_magnitude_stats.csv"
MOMENTS = ("mean", "std_dev", "variance", "skewness", "kurtosis")


def _routes() -> dict[str, Callable[[], Any]]:
    text = FIXTURE.read_bytes()
    values = [float(line) for line in text.decode().splitlines()[1:]]
    routes: dict[str, Callable[[], Any]] = {
        engine: (lambda engine=engine: dp.profile(str(FIXTURE), engine=engine))
        for engine in ("auto", "incremental", "columnar")
    }
    routes["bytes"] = lambda: dp.profile(text, format="csv")
    routes["dict"] = lambda: dp.profile({"amount": values})
    routes["rows"] = lambda: dp.profile([{"amount": value} for value in values])
    # An optional library drops only its own route.
    if importlib.util.find_spec("pandas") is not None:
        import pandas as pd

        routes["pandas"] = lambda: dp.profile(pd.DataFrame({"amount": values}))
    if importlib.util.find_spec("polars") is not None:
        import polars as pl

        routes["polars"] = lambda: dp.profile(pl.DataFrame({"amount": values}))
    if importlib.util.find_spec("pyarrow") is not None:
        import pyarrow as pa

        routes["arrow"] = lambda: dp.profile(pa.table({"amount": pa.array(values, pa.float64())}))
    return routes


def test_large_magnitude_moments_serialize_alike_on_every_route():
    observed = {}
    for label, route in _routes().items():
        stats = route().to_dict()["columns"][0]["stats"]
        observed[label] = tuple(stats[name] for name in MOMENTS)
    first_label, first = next(iter(observed.items()))
    assert all(isinstance(value, float) for value in first), first
    wrong = [
        f"[{label}] {moments} != [{first_label}] {first}"
        for label, moments in observed.items()
        if moments != first
    ]
    assert not wrong, "\n".join(wrong)


def test_every_export_rounds_the_moments_as_to_dict_does():
    """``column_to_dict()``, the flat records behind ``to_dataframe()`` and the
    markdown, HTML and text views round in Python, so they must apply the same
    rule as the Rust summary."""
    report = dp.profile(str(FIXTURE))
    summary = report.to_dict()["columns"][0]["stats"]
    expected = {name: summary[name] for name in MOMENTS}
    column = dp.column_to_dict(next(iter(report.column_profiles.values())))
    record = report._records()[0]
    assert {name: column["stats"][name] for name in MOMENTS} == expected
    assert {name: record[name] for name in MOMENTS} == expected
    table = report.to_markdown()
    assert f"mean={expected['mean']}, std={expected['std_dev']}" in table


def test_reports_record_the_statistic_rounding():
    semantics = dp.profile(str(FIXTURE)).metric_semantics
    assert semantics is not None
    assert semantics["statistic_rounding"] == "four_decimals_twelve_significant"
