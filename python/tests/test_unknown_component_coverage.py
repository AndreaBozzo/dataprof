"""A sampled quality component this build does not know (#749).

A report can record a sampled component for a dimension a later release
added, whose weight is in the stored overall score. ``check()`` and
``compare()`` read that score's coverage through one rule, so a check never
certifies a score the delta calls unrecorded. ``tests/unknown_component_coverage.rs``
is the Rust twin.
"""

from __future__ import annotations

import json

import dataprof as dp
import pytest

DATA = {"id": [1, 2, 3, 4], "amount": [10, 20, 30, 40]}
UNRECORDED = {"coverage": "incomplete", "reason": "coverage_unrecorded"}
SAMPLED = {"coverage": "incomplete", "reason": "quality_sampled"}
COMPLETE = {"coverage": "complete"}


def _reloaded(exact: list[str], sampled: list[str]) -> dp.ProfileReport:
    document = json.loads(dp.profile(DATA).to_json())
    document["quality"]["confidence"] = {
        "Mixed": {
            "exact_dimensions": exact,
            "sample_size": 4,
            "sampled_dimensions": sampled,
        }
    }
    return dp.ProfileReport.from_dict(document)


def _evidence(result: dp.QualityGateResult, code: str) -> dict:
    (check,) = [check for check in result.checks if check.code == code]
    return check.evidence


@pytest.mark.parametrize(
    ("exact", "sampled", "overall"),
    [
        (["completeness"], ["future_dimension"], UNRECORDED),
        (["completeness"], ["consistency"], SAMPLED),
        (["completeness", "future_dimension"], [], COMPLETE),
    ],
    ids=["unknown-sampled", "known-sampled", "unknown-exact"],
)
def test_check_and_compare_resolve_component_coverage_alike(exact, sampled, overall):
    report = _reloaded(exact, sampled)

    wrong = []
    gate_overall = _evidence(report.check(min_quality_score=1), "min_quality_score")
    if gate_overall != overall:
        wrong.append(f"check overall: {gate_overall}")
    delta_overall = report.compare(report)["quality_score"]["evidence"]["a"]
    if delta_overall != overall:
        wrong.append(f"compare overall: {delta_overall}")
    # A known dimension's own score is not downgraded by another component.
    completeness = _evidence(
        report.check(min_dimension_scores={"completeness": 1}), "min_dimension_score"
    )
    if completeness != COMPLETE:
        wrong.append(f"check completeness: {completeness}")
    assert not wrong, wrong


def test_an_unknown_sampled_component_makes_a_full_source_minimum_inconclusive():
    report = _reloaded(["completeness"], ["future_dimension"])

    assert report.check(min_quality_score=10).verdict == "inconclusive"
    assert report.check(min_quality_score=10, scope="observed").verdict == "pass"
