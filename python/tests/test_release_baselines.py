"""Each file of ``tests/fixtures/release_baselines/`` gated against the report the
last release wrote for it, with no score drop allowed (#1245).

A fix that moves a score on these unchanged bytes has to record a
``metric_semantics`` definition, so that a gate against an older baseline reads
``definition_not_comparable`` instead of failing on identical data (#1237 and
#1243 were found by hand after merge). After each release,
``.github/scripts/release_baselines.py <version>`` rewrites the reports from the
published wheel.

``tests/release_baselines.rs`` is the Rust twin.
"""

from __future__ import annotations

import json
from pathlib import Path

import dataprof as dp

CORPUS = Path(__file__).parents[2] / "tests/fixtures/release_baselines"
DIMENSIONS = (
    "completeness",
    "consistency",
    "uniqueness",
    "accuracy",
    "timeliness",
    "validity",
    "precision",
)
#: Reasons a rule is left undecided that say nothing about a score moving.
ACCOUNTED = {"definition_not_comparable", "setting_changed", "setting_unrecorded"}


def _score(report: dp.ProfileReport, dimension: str | None) -> float | None:
    quality = report.to_dict()["quality"] or {}
    return (
        quality.get("dimension_scores", {}).get(dimension)
        if dimension
        else quality.get("overall_score")
    )


def test_no_score_moves_against_the_last_release_without_a_definition():
    release = (CORPUS / "RELEASE").read_text(encoding="utf-8").strip()
    inputs = sorted(
        p for p in CORPUS.iterdir() if p.name != "RELEASE" and not p.name.endswith(".report.json")
    )
    assert len(inputs) >= 7, inputs

    wrong = []
    for path in inputs:
        baseline = dp.ProfileReport.load(CORPUS / f"{path.name}.report.json")
        report = dp.profile(str(path))
        result = report.check(
            baseline=baseline,
            max_quality_score_drop=0,
            max_dimension_score_drops=dict.fromkeys(DIMENSIONS, 0),
        )
        for check in result.checks:
            reason = (check.reason or {}).get("reason")
            if check.status == "passed" or reason in ACCOUNTED:
                continue
            # A dimension neither release scores on this file has nothing to move.
            if (
                reason in {"not_assessed", "not_in_baseline"}
                and check.dimension
                and _score(report, check.dimension) is None
                and _score(baseline, check.dimension) is None
            ):
                continue
            before, after = _score(baseline, check.dimension), _score(report, check.dimension)
            wrong.append(
                f"{path.name}: {check.code} {check.dimension or 'overall'}: "
                f"{reason or check.status}, {before} -> {after}: {check.message}"
            )
    assert not wrong, (
        f"scores moved against the {release} reports with no metric_semantics definition "
        "recording why; add one to MetricSemantics::current() and score_definitions:\n"
        + "\n".join(wrong)
    )


#: Definitions no CSV or Parquet input of the corpus can reach (#1317).
UNREACHED = {"json_number_reading", "sampled_score_estimator", "outlier_spread"}


def test_csv_and_parquet_decide_accuracy_and_overall_without_unreached_definitions():
    # The CSV and Parquet inputs hold no JSON number, are read in full, and have
    # no numeric column whose quartiles coincide (#1317). Given every other
    # definition the report records, so that only those three can be missing,
    # no accuracy or overall rule is held by a definition: each passes or fails
    # (accuracy_comparison does move them against 0.13), or waits on a setting
    # the release did not record, such as timeliness_clock.
    inputs = sorted(p for p in CORPUS.iterdir() if p.suffix in {".csv", ".parquet"})
    assert len(inputs) >= 5, inputs

    held = []
    for path in inputs:
        report = dp.profile(str(path))
        current = json.loads(report.to_json())["metric_semantics"]
        document = json.loads((CORPUS / f"{path.name}.report.json").read_text(encoding="utf-8"))
        document["metric_semantics"].update(
            {key: value for key, value in current.items() if key not in UNREACHED}
        )
        result = report.check(
            baseline=dp.ProfileReport.from_json(json.dumps(document)),
            max_quality_score_drop=0,
            max_dimension_score_drops={"accuracy": 0},
        )
        for check in result.checks:
            reason = check.reason or {}
            if reason.get("reason") == "definition_not_comparable":
                held.append(f"{path.name}: {check.code} {check.dimension or 'overall'}: {reason}")
    assert not held, "\n".join(held)
