"""A baseline saved before the accuracy score compared exact values (#1199)
is not comparable on the rules that read accuracy (#1237).

``tests/accuracy_comparison.rs`` is the Rust twin.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import dataprof as dp

FIXTURES = Path(__file__).parents[2] / "tests/fixtures"
# The bytes accuracy_0_13_1.report.json was profiled from with the published
# 0.13.1 wheel. The first value breaks the percentage range only when compared
# by its digits, so the current release scores accuracy 75 where 0.13.1 scored
# 100.
BATCH = "discount_percent\n100.000000000000000001\n100\n0\n50\n"
POLICY: dict[str, Any] = {
    "max_quality_score_drop": 0,
    "max_dimension_score_drops": {"accuracy": 0, "completeness": 0},
}


def _profile(tmp_path: Path) -> dp.ProfileReport:
    path = tmp_path / "batch.csv"
    path.write_text(BATCH, encoding="utf-8")
    return dp.profile(str(path))


def _find(result, code, dimension=None):
    for check in result.checks:
        if check.code == code and dimension in (None, check.dimension):
            return check
    raise AssertionError(f"no {code} check for {dimension}")


def test_a_0_13_1_baseline_leaves_the_accuracy_reading_rules_not_comparable(tmp_path: Path):
    baseline = dp.ProfileReport.load(FIXTURES / "accuracy_0_13_1.report.json")
    report = _profile(tmp_path)
    # The fixture is what the issue describes: the same bytes, scored higher.
    assert baseline.quality_score == 98.44
    assert report.quality_score is not None and report.quality_score < 98.44

    result = report.check(baseline=baseline, **POLICY)
    unevaluated = {"reason": "definition_not_comparable", "definition": "accuracy_comparison"}
    wrong = [
        (code, dimension, check.to_dict())
        for code, dimension in (
            ("max_quality_score_drop", None),
            ("max_dimension_score_drop", "accuracy"),
        )
        if (check := _find(result, code, dimension)).reason != unevaluated
    ]
    assert wrong == []
    assert _find(result, "max_dimension_score_drop", "completeness").status == "passed"
    assert result.verdict == "inconclusive"


def test_a_baseline_from_the_same_build_decides_the_accuracy_reading_rules(tmp_path: Path):
    baseline = _profile(tmp_path)
    result = _profile(tmp_path).check(baseline=baseline, **POLICY)
    assert [check.status for check in result.checks] == ["passed"] * len(result.checks)
    assert result.verdict == "pass"
