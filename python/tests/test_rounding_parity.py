"""Python must round exactly as the Rust layer does (#513).

The two layers used to disagree three ways, and all three produced different
numbers for the same data:

1. **Different precisions.** ``min``, ``max``, ``median``, ``mode`` and
   ``avg_length`` were 2dp in Rust and 4dp in Python;
   ``coefficient_of_variation`` was the reverse.
2. **Rounded on one side only.** Every float in the seven quality dimension
   dicts carries ``round_2`` in Rust; Python passed them through raw.
3. **Different tie-breaking.** Rust rounds the stored ``f64``; Python rounded
   the shortest decimal string that prints for it.

Two shared fixtures pin the outcome, and the Rust tests
(``tests/rounding_parity.rs``, ``tests/report_rounding_parity.rs``) assert
against the same two files. Changing either implementation alone fails that
layer's test rather than drifting unnoticed.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import dataprof
import pytest

# The rounding helpers are deliberately private and absent from __init__.pyi, so
# they are not part of the declared surface #514 pins. Reaching them here is
# intentional: the convention has to be exercised on arbitrary values, which no
# public accessor allows.
from dataprof import _r2, _r4  # ty: ignore[unresolved-import]

_FIXTURES = Path(__file__).resolve().parents[2] / "tests" / "fixtures"
_CONVENTION = "round the stored f64 at n decimal places, ties away from zero"

# Rounded floats only. Counts and names are integers or strings and cannot
# disagree about rounding.
_FLOAT_STATS = frozenset(
    {
        "min",
        "max",
        "mean",
        "std_dev",
        "variance",
        "median",
        "mode",
        "skewness",
        "kurtosis",
        "coefficient_of_variation",
        "avg_length",
        "true_ratio",
    }
)


def _load(name: str) -> dict[str, Any]:
    path = _FIXTURES / name
    assert path.exists(), f"missing shared fixture {path}"
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def rounding_cases() -> dict[str, Any]:
    return _load("rounding_parity.json")


@pytest.fixture(scope="module")
def report_expectations() -> dict[str, Any]:
    return _load("report_rounding_parity.json")


@pytest.fixture(scope="module")
def report():
    return dataprof.profile_file(_FIXTURES / "rounding_parity.csv")


def test_helpers_match_the_shared_rounding_fixture(rounding_cases):
    """_r2/_r4 produce exactly what the Rust serializers produce."""
    assert rounding_cases["convention"] == _CONVENTION, (
        "the fixture states a convention this test was not written against"
    )
    cases = rounding_cases["cases"]
    assert cases, "fixture lists no cases, so this test would assert nothing"

    failures = []
    for case in cases:
        value = case["value"]
        for helper, key in ((_r2, "r2"), (_r4, "r4")):
            actual = helper(value)
            if actual != case[key]:
                failures.append(
                    f"{helper.__name__}({value!r}): expected {case[key]!r}, got {actual!r}"
                )
    assert not failures, (
        f"{len(failures)} of {len(cases)} fixture cases disagree with the Python helpers:\n"
        + "\n".join(failures)
    )


def test_fixture_covers_the_ties_that_discriminate_the_rule(rounding_cases):
    """A fixture of well-behaved values would pass under either rule.

    Exact decimal ties are the only place the two tie-breaking algorithms can
    disagree, so if they ever fall out of the fixture it stops guarding the
    thing it exists for.
    """
    ties = [
        case["value"]
        for case in rounding_cases["cases"]
        if len(repr(case["value"]).partition(".")[2]) == 3 and repr(case["value"]).endswith("5")
    ]
    assert len(ties) >= 10, (
        f"fixture holds only {len(ties)} exact-tie cases; it no longer "
        "discriminates the tie-breaking rule it exists to pin"
    )


def test_ties_round_the_stored_float_not_its_printed_form():
    """The decision itself, stated on the case the issue was written around.

    ``23 / 4000 * 100`` prints as ``0.575`` but is stored just below it. Rounding
    the printed string gives 0.58; rounding the stored value gives 0.57, and the
    stored value is what both layers now report.
    """
    value = 23 / 4000 * 100
    assert repr(value) == "0.575", "the premise of this test no longer holds"
    # Decimal(float) is the float's exact value. The literal 0.575 parses to
    # this same float, so comparing them as floats proves nothing — the gap is
    # only visible in exact decimal arithmetic.
    assert Decimal(value) < Decimal("0.575"), (
        "0.575 is not representable; the stored value must be below it"
    )
    assert _r2(value) == 0.57


def _column_floats(columns: dict[str, Any]) -> dict[str, float]:
    """Flatten a fixture or report into ``column.field`` -> value."""
    flat: dict[str, float] = {}
    for name, fields in columns.items():
        for field, value in fields.items():
            if field == "quartiles" and isinstance(value, dict):
                for quartile, number in value.items():
                    flat[f"{name}.quartiles.{quartile}"] = number
            elif isinstance(value, float):
                flat[f"{name}.{field}"] = value
    return flat


def test_report_columns_match_the_shared_expectations(report, report_expectations):
    """to_dict() emits the same rounded floats the Rust report does."""
    document = report.to_dict()
    actual_columns = {}
    for column in document["columns"]:
        stats = column.get("stats") or {}
        entry: dict[str, Any] = {k: v for k, v in stats.items() if k in _FLOAT_STATS}
        if isinstance(stats.get("quartiles"), dict):
            entry["quartiles"] = stats["quartiles"]
        actual_columns[column["name"]] = entry

    expected = _column_floats(report_expectations["columns"])
    actual = _column_floats(actual_columns)
    assert expected, "fixture lists no column floats, so this test would assert nothing"

    failures = [
        f"{field}: expected {want!r}, Python emitted {actual.get(field)!r}"
        for field, want in expected.items()
        if actual.get(field) != want
    ]
    assert not failures, (
        f"{len(failures)} of {len(expected)} shared fields disagree between "
        f"to_dict() and the fixture Rust is also held to:\n" + "\n".join(failures)
    )


def test_report_quality_matches_the_shared_expectations(report, report_expectations):
    """Quality dimension floats are rounded, and rounded the same way."""
    quality = report.to_dict()["quality"]
    expected = report_expectations["quality"]
    assert expected, "fixture lists no quality floats, so this test would assert nothing"

    failures = []
    for dimension, fields in expected.items():
        emitted = quality.get(dimension) or {}
        for field, want in fields.items():
            if emitted.get(field) != want:
                failures.append(
                    f"{dimension}.{field}: expected {want!r}, Python emitted {emitted.get(field)!r}"
                )
    assert not failures, (
        f"{len(failures)} quality fields disagree between to_dict() and the "
        f"fixture Rust is also held to:\n" + "\n".join(failures)
    )


def test_quality_dimension_floats_are_actually_rounded(report):
    """The specific defect: Python emitted raw floats where Rust rounded.

    Reported as 4.833333333333333 against Rust's 4.83. Any float in a dimension
    dict must now be a 2dp value.
    """
    quality = report.to_dict()["quality"]
    for dimension in (
        "completeness",
        "consistency",
        "uniqueness",
        "accuracy",
        "timeliness",
        "validity",
        "precision",
    ):
        for field, value in (quality.get(dimension) or {}).items():
            if isinstance(value, float):
                assert value == _r2(value), f"{dimension}.{field} = {value!r} is not rounded to 2dp"


def test_percentages_and_statistics_use_their_own_precision(report):
    """coefficient_of_variation is a percentage; the stats beside it are not.

    Pins the per-field half of the convention: it is chosen by what the number
    is, not by which struct it happens to live in.
    """
    amount = next(c for c in report.to_dict()["columns"] if c["name"] == "amount")
    stats = amount["stats"]
    assert stats["coefficient_of_variation"] == _r2(stats["coefficient_of_variation"])
    # min/max/median are data values and must survive past the second decimal,
    # which is exactly what the old 2dp Rust serialization destroyed.
    assert stats["min"] == 1.0001
    assert stats["max"] == 5.9382
    assert stats["median"] == 3.4691
