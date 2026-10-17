"""Adjusted sample statistics across every route; Rust twin: tests/statistics_estimators.rs."""

from __future__ import annotations

from pathlib import Path

import pytest

try:
    from _profile_routes import _routes
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )


FIXTURES = {
    "asymmetric": (["1", "2", "3", "10", "4", "5"], 1.4396, 2.4377, 76.52),
    "small_units": ([str(v * 1e-12) for v in [1, 2, 3, 10, 4, 5]], 1.4396, 2.4377, 76.52),
    "constant": (["2"] * 4, 0.0, 0.0, 0.0),
    "two": (["1", "2"], None, None, 47.14),
    "three": (["1", "2", "3"], 0.0, None, 50.0),
    "zero_mean": (["-1", "0", "1"], 0.0, None, None),
}


@pytest.mark.parametrize(
    "field", ["skewness", "kurtosis", "coefficient_of_variation", "is_approximate"]
)
def test_every_route_uses_the_statistics_estimator(tmp_path: Path, field: str):
    wrong = []
    for label, route in _routes(tmp_path).items():
        for index, (fixture, (values, skew, kurt, cv)) in enumerate(FIXTURES.items()):
            expected = {
                "skewness": skew,
                "kurtosis": kurt,
                "coefficient_of_variation": cv,
                "is_approximate": False,
            }[field]
            report = route(f"f{index}", values)
            stats = report.to_dict()["columns"][0]["stats"]
            actual = stats.get(field)
            if actual != expected or (expected is None and field in stats):
                wrong.append(f"[{label}] {fixture}: {field}={actual!r}, expected {expected!r}")
    assert not wrong, "\n".join(wrong)
