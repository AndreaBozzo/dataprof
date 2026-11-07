"""A baseline saved before JSON numbers were read from their literal digits
(#1166) is not comparable on the rules that read them (#1243).

``tests/json_number_reading.rs`` is the Rust twin.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import dataprof as dp

FIXTURES = Path(__file__).parents[2] / "tests/fixtures"
# The bytes json_number_reading_0_13_1.report.json was profiled from with the
# published 0.13.1 wheel. 0.13.1 read the three long amounts as one f64 with no
# decimals, so it scored precision 100 and found two duplicate rows; their
# digits score precision 75 and no duplicates.
BATCH = (
    '{"amount":1000000000000000.01,"n":1}\n'
    '{"amount":1000000000000000.02,"n":1}\n'
    '{"amount":1000000000000000.03,"n":1}\n'
    '{"amount":7,"n":2}\n'
)
READING = ("precision", "uniqueness")
DECIDED = ("completeness", "consistency")
POLICY: dict[str, Any] = {
    "max_quality_score_drop": 0,
    "max_dimension_score_drops": dict.fromkeys((*READING, *DECIDED, "accuracy"), 0),
}


def _profile(tmp_path: Path) -> dp.ProfileReport:
    path = tmp_path / "batch.jsonl"
    path.write_text(BATCH, encoding="utf-8")
    return dp.profile(str(path))


def _find(result, code, dimension=None):
    for check in result.checks:
        if check.code == code and dimension in (None, check.dimension):
            return check
    raise AssertionError(f"no {code} check for {dimension}")


def test_a_0_13_1_baseline_leaves_the_rules_reading_json_numbers_not_comparable(
    tmp_path: Path,
):
    baseline = dp.ProfileReport.load(FIXTURES / "json_number_reading_0_13_1.report.json")
    report = _profile(tmp_path)
    # The fixture is what the issue describes: the same bytes, scored
    # differently on precision and uniqueness.
    scores = [r.to_dict()["quality"]["dimension_scores"] for r in (baseline, report)]
    assert [(s["precision"], s["uniqueness"]) for s in scores] == [(100, 50), (75, 100)]

    result = report.check(baseline=baseline, **POLICY)
    wrong = [
        (dimension, check.to_dict())
        for dimension in READING
        if (check := _find(result, "max_dimension_score_drop", dimension)).reason
        != {"reason": "definition_not_comparable", "definition": "json_number_reading"}
    ]
    # 0.13.1 records neither definition, and these rules name the first one
    # they read, accuracy_comparison (#1237).
    wrong += [
        (code, check.to_dict())
        for code, dimension in (
            ("max_quality_score_drop", None),
            ("max_dimension_score_drop", "accuracy"),
        )
        if ((check := _find(result, code, dimension)).reason or {}).get("reason")
        != "definition_not_comparable"
    ]
    wrong += [
        (dimension, check.to_dict())
        for dimension in DECIDED
        if (check := _find(result, "max_dimension_score_drop", dimension)).status != "passed"
    ]
    assert wrong == []
    assert result.verdict == "inconclusive"


def test_a_baseline_from_the_same_build_decides_the_rules_reading_json_numbers(
    tmp_path: Path,
):
    baseline = _profile(tmp_path)
    result = _profile(tmp_path).check(baseline=baseline, **POLICY)
    assert [check.status for check in result.checks] == ["passed"] * len(result.checks)
    assert result.verdict == "pass"
