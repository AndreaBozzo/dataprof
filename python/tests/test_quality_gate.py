"""``ProfileReport.check()``: policy validation, verdicts, and evidence (#376).

The cross-language contract lives in ``test_quality_gate_parity.py``; this file
covers what only the Python surface can be asked — keyword validation, the
result object's accessors, and a report rebuilt from a saved document.
"""

from __future__ import annotations

from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import Any

import dataprof as dp
import pytest

CSV = (
    "order_id,customer_id,amount\n"
    "A-1,C-1,10\n"
    "A-2,C-2,20\n"
    "A-3,,30\n"
    "A-2,C-2,20\n"
    "A-5,C-3,50\n"
    "A-6,C-4,60\n"
)


@pytest.fixture
def report(tmp_path: Path):
    path = tmp_path / "orders.csv"
    path.write_text(CSV, encoding="utf-8")
    return dp.profile_file(path)


def _check(result, code: str, **match):
    for check in result.checks:
        if check.code != code:
            continue
        if all(getattr(check, key) == value for key, value in match.items()):
            return check
    raise AssertionError(f"no {code} check matching {match} in {result.to_dict()}")


def test_met_requirements_pass(report):
    result = report.check(min_quality_score=90, max_null_percentage={"*": 20})

    assert result.verdict == "pass"
    assert result.passed
    assert bool(result) is True
    assert result.violations == []
    assert result.unevaluated == []


def test_violation_carries_code_observed_expected_and_column(report):
    result = report.check(max_null_percentage={"customer_id": 10})

    assert result.verdict == "fail"
    assert not result.passed
    (violation,) = result.violations
    assert violation.code == "max_null_percentage"
    assert violation.column == "customer_id"
    assert violation.observed == pytest.approx(16.67)
    assert violation.expected == {"comparison": "at_most", "value": 10.0}
    assert violation.is_violation


def test_a_bare_number_sets_the_limit_for_every_column(report):
    bare = report.check(max_null_percentage=10)
    wildcard = report.check(max_null_percentage={"*": 10})

    assert bare.to_dict() == wildcard.to_dict()
    assert {check.column for check in bare.checks} == {"order_id", "customer_id", "amount"}


def test_a_named_limit_overrides_the_wildcard(report):
    result = report.check(max_null_percentage={"customer_id": 50, "*": 0})

    assert result.verdict == "pass"
    assert _check(result, "max_null_percentage", column="customer_id").expected["value"] == 50.0
    assert _check(result, "max_null_percentage", column="amount").expected["value"] == 0.0


def test_iterating_the_result_yields_its_checks(report):
    result = report.check(max_null_percentage={"*": 50})

    assert list(result) == list(result.checks)
    assert len(result.checks) == 3


def test_result_serializes_to_json(report):
    import json

    result = report.check(min_quality_score=90)
    assert json.loads(result.to_json()) == result.to_dict()


@pytest.mark.parametrize(
    "policy",
    [
        {"min_quality_score": 101},
        {"min_quality_score": -1},
        # Text is not a threshold, even when it spells one (#771).
        {"min_quality_score": "90"},
        {"max_null_percentage": {"amount": "20"}},
        {"min_dimension_scores": {"completeness": "90"}},
        # Too large for a float: a ValueError, not an OverflowError (#771).
        {"min_quality_score": 10**400},
        {"min_quality_score": float("nan")},
        {"max_null_percentage": {"amount": 200}},
        {"min_dimension_scores": {"completeness": 150}},
        {"min_dimension_scores": {"not_a_dimension": 90}},
        {"require_metrics": ["not_a_dimension"]},
        {"max_duplicate_rows": -1},
        {"max_duplicate_rows": 1.5},
        {"min_quality_score": 90, "scope": "whatever"},
    ],
)
def test_an_unevaluable_policy_raises_rather_than_failing_the_dataset(report, policy):
    """A misconfigured gate is not a failing dataset, and a threshold written
    as a 0..1 ratio must not silently become a gate that never fires."""
    with pytest.raises(ValueError):
        report.check(**policy)


def test_every_real_number_type_is_a_threshold(report):
    """Refusing text must not refuse the numeric types callers pass (#771)."""
    import numpy as np

    for value in (90, 90.0, Decimal("90"), Fraction(90), np.float64(90), np.int64(90)):
        assert report.check(min_quality_score=value).to_dict() == (
            report.check(min_quality_score=90).to_dict()
        ), type(value)


def test_an_empty_policy_is_rejected(report):
    with pytest.raises(ValueError, match="states no requirement"):
        report.check()


def test_unanalyzed_quality_is_inconclusive_not_a_pass(tmp_path: Path):
    path = tmp_path / "orders.csv"
    path.write_text(CSV, encoding="utf-8")
    report = dp.profile_file(path, metrics=["schema"])

    result = report.check(min_quality_score=90)
    assert result.verdict == "inconclusive"
    assert not result.passed
    check = _check(result, "min_quality_score")
    assert check.observed is None
    assert check.reason == {"reason": "quality_unavailable", "quality_status": "not_requested"}


def test_require_metrics_turns_absence_into_a_violation(tmp_path: Path):
    path = tmp_path / "orders.csv"
    path.write_text(CSV, encoding="utf-8")
    report = dp.profile_file(path, metrics=["schema"])

    result = report.check(min_quality_score=90, require_metrics=["quality"])
    assert result.verdict == "fail"
    assert _check(result, "require_metric").message == "the report carries no quality assessment"


def test_require_metrics_accepts_a_dimension(report):
    result = report.check(require_metrics=["timeliness"])

    assert result.verdict == "fail"
    check = _check(result, "require_metric", dimension="timeliness")
    assert check.message == "the required dimension was not assessed"


def test_a_saved_report_gates_the_same_way(report, tmp_path: Path):
    """A report reloaded from its own document must reach the same verdict;
    otherwise a baseline saved yesterday cannot be gated today."""
    reloaded = dp.ProfileReport.from_dict(report.to_dict())
    policy: dict[str, Any] = {
        "min_quality_score": 90,
        "max_null_percentage": {"customer_id": 10, "*": 20},
    }

    assert reloaded.check(**policy).to_dict() == report.check(**policy).to_dict()


def test_a_document_without_recorded_coverage_is_not_read_as_a_full_scan(report):
    """A report saved before dataprof recorded how its quality numbers were
    obtained does not say whether they cover every scanned row. Unknown
    coverage is not full coverage, so a full-source policy cannot pass on it.
    """
    document = report.to_dict()
    assert document["quality"].pop("sampled_dimensions") == []
    legacy = dp.ProfileReport.from_dict(document)

    assert legacy.quality_sampled_dimensions is None
    result = legacy.check(min_quality_score=90)
    assert result.verdict == "inconclusive"
    check = _check(result, "min_quality_score")
    assert check.evidence == {"coverage": "incomplete", "reason": "coverage_unrecorded"}
    assert check.reason == {"reason": "evidence_incomplete", "gap": "coverage_unrecorded"}

    # The same report, asked about what was measured, is still decidable.
    assert legacy.check(min_quality_score=90, scope="observed").verdict == "pass"


def test_a_zero_weighted_sampled_dimension_does_not_taint_the_overall_score(report):
    """Weights say what reaches the aggregate, not what was measured.

    The overall score renormalizes over the assessed dimensions, so a sampled
    dimension the weights exclude cannot move it. Withholding the aggregate
    because of one would refuse a verdict the number does not depend on.
    """
    document = report.to_dict()
    document["quality"]["sampled_dimensions"] = ["consistency"]
    # `assessed_dimensions` is the weighted contributing set, and consistency
    # is not in it once its weight is zero.
    document["quality"]["assessed_dimensions"] = ["completeness"]
    loaded = dp.ProfileReport.from_dict(document)

    result = loaded.check(min_quality_score=1, min_dimension_scores={"consistency": 1})
    assert _check(result, "min_quality_score").evidence == {"coverage": "complete"}
    assert _check(result, "min_dimension_score").evidence == {
        "coverage": "incomplete",
        "reason": "quality_sampled",
    }


def test_quality_sampled_dimensions_round_trips(report):
    assert report.quality_sampled_dimensions == []
    assert report.to_dict()["quality"]["sampled_dimensions"] == []
    assert dp.ProfileReport.from_dict(report.to_dict()).quality_sampled_dimensions == []


def test_a_fully_read_large_file_still_has_a_sampled_quality_score(tmp_path: Path):
    """The case an execution-metadata-only gate gets wrong.

    Quality metrics come from a bounded reservoir (10,000 values per column),
    so a file larger than that carries a sampled score even though the source
    was exhausted and no row sampler ran. ``source_exhausted`` and
    ``sampling_applied`` both say the scan covered everything, and they are
    both right -- about the scan. A policy about the whole source has to read
    the metric's own provenance instead.
    """
    path = tmp_path / "wide.csv"
    rows = "\n".join(f"K-{n},{n % 7}" for n in range(12_000))
    path.write_text(f"key,bucket\n{rows}\n", encoding="utf-8")

    report = dp.profile_file(path)
    assert report.source_exhausted
    assert not report.sampling_applied
    assert report.quality_sampled_dimensions, (
        "the reservoir did not bind; this test no longer reaches the case it guards"
    )

    result = report.check(min_quality_score=1)
    assert result.verdict == "inconclusive"
    assert result.evidence == {"coverage": "complete"}
    check = _check(result, "min_quality_score")
    assert check.evidence == {"coverage": "incomplete", "reason": "quality_sampled"}
    assert check.reason == {"reason": "evidence_incomplete", "gap": "quality_sampled"}

    # Asked about what the metrics measured, the same report is decidable.
    assert report.check(min_quality_score=1, scope="observed").verdict == "pass"


def test_a_duplicate_column_name_resolves_to_the_first_profile(report):
    """Both layers read the first profile of a repeated name.

    Most input paths reject duplicate column names outright, but a report
    rebuilt from a document can carry them, and the two evaluators picking
    different profiles would be a silent cross-language disagreement.
    """
    document = report.to_dict()
    first, second = dict(document["columns"][0]), dict(document["columns"][0])
    first["null_count"], first["null_percentage"] = 0, 0.0
    second["null_count"], second["null_percentage"] = second["total_count"], 100.0
    document["columns"] = [first, second, *document["columns"][1:]]

    result = dp.ProfileReport.from_dict(document).check(max_null_percentage={first["name"]: 50})
    assert _check(result, "max_null_percentage", column=first["name"]).observed == 0.0
    assert result.verdict == "pass"


def test_a_boolean_is_not_a_percentage(report):
    """`True` is 1.0 to `float()`, which would quietly become a 1% floor."""
    with pytest.raises(ValueError):
        report.check(min_quality_score=True)
