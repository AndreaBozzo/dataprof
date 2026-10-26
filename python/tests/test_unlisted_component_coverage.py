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
    summary = report.to_dict()
    summary["quality"].pop("exact_dimensions", None)
    flat = dp.ProfileReport.from_dict(summary)

    assert flat.findings().not_evaluated == report.findings().not_evaluated


def test_flat_summary_preserves_exact_components_and_unlisted_coverage(tmp_path):
    document = json.loads(dp.profile(DATED).to_json())
    exact = ["completeness", "duplicate_rows", "future_component"]
    document["quality"]["confidence"] = {
        "Mixed": {
            "exact_dimensions": exact,
            "sampled_dimensions": ["consistency", "accuracy", "validity", "precision"],
            "sample_size": 4,
        }
    }
    report = dp.ProfileReport.from_dict(document)
    summary = report.to_dict()
    assert summary["quality"]["exact_dimensions"] == exact
    flat = dp.ProfileReport.from_dict(summary)
    path = tmp_path / "summary.json"
    flat.save(path)
    for restored in (flat, dp.ProfileReport.load(path), dp.ProfileReport.from_dict(flat.to_dict())):
        assert restored.quality_exact_dimensions == exact
        assert restored.to_dict()["quality"]["exact_dimensions"] == exact
        assert restored._quality_unlisted_components == ["timeliness"]
        delta = restored.compare(restored)
        assert delta["quality_score"]["evidence"]["a"] == UNRECORDED
        assert delta["dimensions"]["timeliness"]["evidence"]["a"] == UNRECORDED
        assert delta["dimensions"]["completeness"]["evidence"]["a"] == COMPLETE
        assert delta["dimensions"]["consistency"]["evidence"]["a"] == SAMPLED
        assert restored.findings().not_evaluated == report.findings().not_evaluated

    del summary["quality"]["exact_dimensions"]
    legacy = dp.ProfileReport.from_dict(summary)
    assert legacy.quality_exact_dimensions is None
    assert "exact_dimensions" not in legacy.to_dict()["quality"]
    assert legacy.compare(legacy)["dimensions"]["timeliness"]["evidence"]["a"] == COMPLETE


@pytest.mark.parametrize("value", ["completeness", 1, {}, [1], [None]])
def test_flat_summary_rejects_malformed_exact_dimensions(value):
    summary = dp.profile(DATED).to_dict()
    summary["quality"]["exact_dimensions"] = value
    with pytest.raises(ValueError, match="exact_dimensions"):
        dp.ProfileReport.from_dict(summary)


@pytest.mark.parametrize("missing", ["exact_dimensions", "sampled_dimensions"])
def test_missing_coverage_list_preserves_legacy_reading(missing):
    summary = dp.profile(DATED).to_dict()
    summary["quality"]["exact_dimensions"] = []
    summary["quality"]["sampled_dimensions"] = []
    del summary["quality"][missing]
    restored = dp.ProfileReport.from_dict(summary)
    assert restored._quality_unlisted_components == []
    assert missing not in restored.to_dict()["quality"]
    expected = COMPLETE if missing == "exact_dimensions" else UNRECORDED
    assert restored.compare(restored)["dimensions"]["timeliness"]["evidence"]["a"] == expected


@pytest.mark.parametrize("key_values_checked", [0, 4, None])
def test_flat_summary_uses_the_canonical_key_signal_rule(key_values_checked):
    document = json.loads(dp.profile(DATED).to_json())
    metrics = document["quality"]["metrics"]
    metrics["uniqueness"]["key_column"] = "v"
    metrics["uniqueness"]["key_values_checked"] = key_values_checked
    exact = [
        "completeness",
        "consistency",
        "duplicate_rows",
        "accuracy",
        "timeliness",
        "validity",
        "precision",
    ]
    document["quality"]["confidence"] = {
        "Mixed": {
            "exact_dimensions": exact,
            "sampled_dimensions": [],
            "sample_size": 4,
        }
    }
    native = dp.ProfileReport.from_dict(document)
    flat = dp.ProfileReport.from_dict(native.to_dict())
    assert flat._quality_unlisted_components == native._quality_unlisted_components
    assert flat._quality_unlisted_components == (
        [] if key_values_checked == 0 else ["key_uniqueness"]
    )
    assert (
        flat.compare(flat)["dimensions"]["uniqueness"]["evidence"]
        == (native.compare(native)["dimensions"]["uniqueness"]["evidence"])
    )


@pytest.mark.parametrize(
    "confidence",
    [
        "Exact",
        "NotAssessed",
        "Unrecorded",
        {"Approximate": {"sample_size": 4, "population_size": 10}},
    ],
)
def test_flat_coverage_round_trip_for_each_confidence(confidence):
    document = json.loads(dp.profile(DATED).to_json())
    document["quality"]["confidence"] = confidence
    if confidence == "NotAssessed":
        document["quality"] = {"metrics": {}, "confidence": confidence}
    native = dp.ProfileReport.from_dict(document)
    summary = native.to_dict()
    flat = dp.ProfileReport.from_dict(summary)
    assert flat.to_dict() == summary
    assert flat.quality_exact_dimensions == native.quality_exact_dimensions
    assert flat.quality_sampled_dimensions == native.quality_sampled_dimensions
    assert flat._quality_unlisted_components == native._quality_unlisted_components == []
    assert flat.compare(flat)["dimensions"] == native.compare(native)["dimensions"]


def test_null_exact_dimensions_reads_as_unrecorded_and_stays_absent_on_resave():
    summary = dp.profile(DATED).to_dict()
    summary["quality"]["exact_dimensions"] = None
    flat = dp.ProfileReport.from_dict(summary)
    assert flat.quality_exact_dimensions is None
    assert "exact_dimensions" not in flat.to_dict()["quality"]
