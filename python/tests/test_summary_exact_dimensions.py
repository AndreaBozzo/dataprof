"""The flat summary records ``exact_dimensions`` beside ``sampled_dimensions`` (#1027).

With both lists recorded, a summary's readers apply the full document's rule:
a component carrying signal that neither list names has unrecorded coverage.
A summary written before the field reads as before. ``tests/summary_exact_dimensions.rs``
is the Rust twin.
"""

from __future__ import annotations

import dataprof as dp
import pytest

DATED = {"event_date": ["2026-01-01"] * 4, "v": [1, 2, 3, 4]}
UNRECORDED = {"coverage": "incomplete", "reason": "coverage_unrecorded"}
SAMPLED = {"coverage": "incomplete", "reason": "quality_sampled"}
COMPLETE = {"coverage": "complete"}


def _coverage(report: dp.ProfileReport) -> dict:
    """Every coverage a reader of the report states."""
    delta = report.compare(report)
    observed = {"compare overall": delta["quality_score"]["evidence"]["a"]}
    for name, change in delta["dimensions"].items():
        observed[f"compare {name}"] = change["evidence"]["a"]
    observed["findings not_evaluated"] = report.findings().not_evaluated
    return observed


@pytest.mark.parametrize(
    "data",
    [
        DATED,
        {"id": [1, 2, 3, 4], "amount": [10, 20, 30, 40]},
        {"a": [1, None, 3, 4], "b": [None, "x", "y", "z"]},
    ],
    ids=["dated", "clean", "a-null-in-every-column"],
)
def test_a_summary_this_build_writes_reads_as_its_report(data):
    report = dp.profile(data)
    summary = report.to_dict()
    assert isinstance(summary["quality"]["exact_dimensions"], list)
    loaded = dp.ProfileReport.from_dict(summary)
    # The loaded summary exports the list it read.
    assert loaded.to_dict()["quality"]["exact_dimensions"] == summary["quality"]["exact_dimensions"]
    assert _coverage(loaded) == _coverage(report)


def _listed_without_timeliness(*, exact: bool) -> dict:
    summary = dp.profile(DATED).to_dict()
    summary["quality"]["exact_dimensions"] = ["completeness", "duplicate_rows"]
    summary["quality"]["sampled_dimensions"] = ["consistency", "accuracy", "validity", "precision"]
    if not exact:
        del summary["quality"]["exact_dimensions"]
    return summary


@pytest.mark.parametrize(
    ("exact", "unlisted", "overall", "drop", "future_dates"),
    [
        (True, UNRECORDED, UNRECORDED, "not_evaluated", ["unrecorded"]),
        (False, COMPLETE, SAMPLED, "passed", []),
    ],
    ids=["recorded", "legacy"],
)
def test_a_component_listed_nowhere_in_a_summary_reads_unrecorded(
    exact, unlisted, overall, drop, future_dates
):
    current = dp.profile(DATED)
    summary = _listed_without_timeliness(exact=exact)
    report = dp.ProfileReport.from_dict(summary)
    assert ("exact_dimensions" in report.to_dict()["quality"]) is exact

    delta = report.compare(report)
    observed = {
        "compare timeliness": (delta["dimensions"]["timeliness"]["evidence"]["a"], unlisted),
        "compare overall": (delta["quality_score"]["evidence"]["a"], overall),
        "compare completeness": (delta["dimensions"]["completeness"]["evidence"]["a"], COMPLETE),
        "compare consistency": (delta["dimensions"]["consistency"]["evidence"]["a"], SAMPLED),
    }
    wrong = {name: got for name, (got, expected) in observed.items() if got != expected}
    (check,) = current.check(baseline=report, max_dimension_score_drops={"timeliness": 1}).checks
    if check.status != drop:
        wrong["check timeliness drop"] = check.status
    reasons = [
        entry["reason"]
        for entry in report.findings().not_evaluated
        if entry["code"] == "future_dates"
    ]
    if reasons != future_dates:
        wrong["findings future_dates"] = reasons
    assert not wrong, wrong


def test_findings_read_a_summary_that_compare_cannot():
    # from_dict() accepts a null `source`, which compare() refuses; findings
    # read only the summary's quality, so they keep working.
    summary = _listed_without_timeliness(exact=True)
    summary["source"] = None
    report = dp.ProfileReport.from_dict(summary)
    with pytest.raises(ValueError, match="cannot be compared"):
        report.compare(report)
    reasons = [
        entry["reason"]
        for entry in report.findings().not_evaluated
        if entry["code"] == "future_dates"
    ]
    assert reasons == ["unrecorded"]


@pytest.mark.parametrize("value", ["completeness", [1], {"a": 1}])
def test_a_malformed_exact_list_is_refused(value):
    summary = dp.profile(DATED).to_dict()
    summary["quality"]["exact_dimensions"] = value
    with pytest.raises(ValueError, match="quality.exact_dimensions"):
        dp.ProfileReport.from_dict(summary)
