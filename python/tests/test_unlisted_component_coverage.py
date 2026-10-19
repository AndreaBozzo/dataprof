"""A computed quality component listed neither as exact nor as sampled (#749).

Its coverage is not recorded, so ``check()``, ``compare()`` and
``findings()`` all read it as ``coverage_unrecorded`` rather than as exact.
``tests/unlisted_component_coverage.rs`` is the Rust twin.
"""

from __future__ import annotations

import json

import dataprof as dp
import pytest

DATED = {"event_date": ["2026-01-01"] * 4, "v": [1, 2, 3, 4]}
UNRECORDED = {"coverage": "incomplete", "reason": "coverage_unrecorded"}
SAMPLED = {"coverage": "incomplete", "reason": "quality_sampled"}
COMPLETE = {"coverage": "complete"}


def _round_trip(report: dp.ProfileReport) -> dp.ProfileReport:
    return dp.ProfileReport.from_dict(json.loads(report.to_json()))


@pytest.mark.parametrize(
    "data",
    [
        DATED,
        {"id": [1, 2, 3, 4], "amount": [10, 20, 30, 40]},
        {"a": [1, None, 3, 4], "b": [None, "x", "y", "z"]},
    ],
    ids=["dated", "clean", "a-null-in-every-column"],
)
def test_a_report_this_build_writes_reads_the_same_after_a_round_trip(data):
    report = dp.profile(data)
    reloaded = _round_trip(report)

    def coverage(r: dp.ProfileReport) -> list:
        dims = ["completeness", "consistency", "uniqueness", "timeliness"]
        checks = r.check(min_quality_score=1, min_dimension_scores=dict.fromkeys(dims, 1)).checks
        return [check.evidence for check in checks] + [r.findings().not_evaluated]

    assert coverage(reloaded) == coverage(report)


def test_a_component_listed_nowhere_reads_unrecorded_in_every_reader():
    document = json.loads(dp.profile(DATED).to_json())
    document["quality"]["confidence"] = {
        "Mixed": {
            "exact_dimensions": ["completeness", "duplicate_rows"],
            "sample_size": 4,
            "sampled_dimensions": ["consistency", "accuracy", "validity", "precision"],
        }
    }
    report = dp.ProfileReport.from_dict(document)

    def dimension(name: str) -> dict:
        result = report.check(min_dimension_scores={name: 1})
        (check,) = result.checks
        return check.evidence

    delta = report.compare(report)
    observed = {
        "check timeliness": (dimension("timeliness"), UNRECORDED),
        "check overall": (report.check(min_quality_score=1).checks[0].evidence, UNRECORDED),
        "check completeness": (dimension("completeness"), COMPLETE),
        "compare timeliness": (delta["dimensions"]["timeliness"]["evidence"]["a"], UNRECORDED),
        "compare overall": (delta["quality_score"]["evidence"]["a"], UNRECORDED),
        "compare consistency": (delta["dimensions"]["consistency"]["evidence"]["a"], SAMPLED),
    }
    wrong = {name: got for name, (got, expected) in observed.items() if got != expected}
    future_dates = [
        entry["reason"]
        for entry in report.findings().not_evaluated
        if entry["code"] == "future_dates"
    ]
    if future_dates != ["unrecorded"]:
        wrong["findings future_dates"] = future_dates
    assert not wrong, wrong


def test_a_flat_summary_keeps_reading_not_sampled_as_exact():
    # The summary records no exact list, so nothing can be missing from both.
    report = dp.profile(DATED)
    flat = dp.ProfileReport.from_dict(report.to_dict())

    assert flat.findings().not_evaluated == report.findings().not_evaluated
