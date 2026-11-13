"""The baseline gate's row count rules, ``max_row_count_drop`` and
``max_row_count_growth`` (#1291).

`tests/row_count_change_gate.rs` is the Rust twin. Here the extra routes are
the Python ones: dicts, pandas and Arrow inputs, CSV bytes, and baselines read
back from ``to_json()`` and from a flat ``to_dict()`` summary, and the
``python -m dataprof.check`` flags and policy-file keys.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import dataprof as dp
import pytest
from dataprof.check import __main__ as entrypoint

pa = pytest.importorskip("pyarrow")
pd = pytest.importorskip("pandas")


def _columns(rows: int) -> dict[str, list[int]]:
    return {"id": list(range(rows)), "v": [i % 7 for i in range(rows)]}


def _csv(rows: int) -> bytes:
    return ("id,v\n" + "".join(f"{i},{i % 7}\n" for i in range(rows))).encode()


ROUTES: dict[str, Callable[[int], dp.ProfileReport]] = {
    "dict": lambda rows: dp.profile(_columns(rows)),
    "pandas": lambda rows: dp.profile(pd.DataFrame(_columns(rows))),
    "arrow": lambda rows: dp.profile(pa.table(_columns(rows))),
    "csv bytes": lambda rows: dp.profile(_csv(rows), format="csv"),
}

BASELINE_FORMS: dict[str, Callable[[dp.ProfileReport], dp.ProfileReport]] = {
    "live": lambda report: report,
    "to_json": lambda report: dp.ProfileReport.from_json(report.to_json()),
    "to_dict": lambda report: dp.ProfileReport.from_dict(report.to_dict()),
}

POLICY: dict[str, Any] = {"max_row_count_drop": 10, "max_row_count_growth": 10}


def _status(result: dp.QualityGateResult, code: str) -> str:
    (check,) = [check for check in result.checks if check.code == code]
    return check.status if check.reason is None else check.reason["reason"]


def _statuses(result: dp.QualityGateResult) -> tuple[str, str]:
    return _status(result, "max_row_count_drop"), _status(result, "max_row_count_growth")


#: Rows against a baseline of 100, with the (drop, growth) statuses.
CASES: list[tuple[str, int, tuple[str, str]]] = [
    ("unchanged", 100, ("passed", "passed")),
    ("a 10% drop, at the allowance", 90, ("passed", "passed")),
    ("an 11% drop", 89, ("failed", "passed")),
    ("a truncation to a fifth", 20, ("failed", "passed")),
    ("10% growth, at the allowance", 110, ("passed", "passed")),
    ("11% growth", 111, ("passed", "failed")),
    ("a doubled file", 200, ("passed", "failed")),
]


def test_a_complete_scan_outside_either_allowance_fails_on_every_route():
    wrong = []
    for route, build in ROUTES.items():
        baseline = build(100)
        for form, read in BASELINE_FORMS.items():
            for case, rows, expected in CASES:
                got = _statuses(build(rows).check(baseline=read(baseline), **POLICY))
                if got != expected:
                    wrong.append(f"{route}/{form}/{case}: {got}, expected {expected}")
    assert not wrong, "\n".join(wrong)


def test_an_incomplete_scan_decides_only_the_outcome_its_unread_rows_cannot_reverse(
    tmp_path: Path,
):
    def scan(rows: int, cap: int | None = None) -> dp.ProfileReport:
        path = tmp_path / f"rows_{rows}.csv"
        path.write_bytes(_csv(rows))
        return dp.profile_file(str(path), max_rows=cap)

    exact = scan(100)
    capped = scan(400, 100)
    incomplete = "evidence_incomplete"
    cases = [
        ("current capped, 50 of 200", scan(200, 50), exact, (incomplete, incomplete)),
        ("current capped, 95 of 200", scan(200, 95), exact, ("passed", incomplete)),
        ("current capped, 150 of 400", scan(400, 150), exact, ("passed", "failed")),
        ("baseline capped, 80 now", scan(80), capped, ("failed", "passed")),
        ("baseline capped, 95 now", scan(95), capped, (incomplete, "passed")),
        ("baseline capped, 200 now", scan(200), capped, (incomplete, incomplete)),
        ("both capped, 100", scan(400, 100), capped, (incomplete, incomplete)),
    ]
    wrong = []
    for case, report, baseline, expected in cases:
        got = _statuses(report.check(baseline=baseline, **POLICY))
        if got != expected:
            wrong.append(f"{case}: {got}, expected {expected}")
        observed = _statuses(report.check(baseline=baseline, scope="observed", **POLICY))
        if incomplete in observed:
            wrong.append(f"{case}, observed scope: {observed}")
    assert not wrong, "\n".join(wrong)


def test_the_check_carries_both_counts_and_the_relative_change():
    result = dp.profile(_columns(200)).check(baseline=dp.profile(_columns(100)), **POLICY)
    assert result.verdict == "fail"
    drop, growth = (check.to_dict() for check in result.checks)
    assert drop["expected"] == {"comparison": "relative_change_at_least", "value": -10.0}
    assert growth["expected"] == {"comparison": "relative_change_at_most", "value": 10.0}
    for check in (drop, growth):
        assert check["observed"] == 200
        assert check["baseline"]["observed"] == 100
        assert check["baseline"]["change"] == 100.0


def test_a_different_column_set_or_an_unrecorded_count_is_never_decided():
    baseline = dp.profile(_columns(100))
    wrong = []
    added = dp.profile({**_columns(100), "w": [1] * 100})
    got = _statuses(added.check(baseline=baseline, **POLICY))
    if got != ("schema_changed", "schema_changed"):
        wrong.append(f"added column: {got}")

    summary = baseline.to_dict()
    del summary["execution"]["rows_processed"]
    got = _statuses(
        dp.profile(_columns(100)).check(baseline=dp.ProfileReport.from_dict(summary), **POLICY)
    )
    if got != ("not_in_baseline", "not_in_baseline"):
        wrong.append(f"summary without rows_processed: {got}")
    assert not wrong, "\n".join(wrong)


def test_the_rules_need_a_baseline_and_a_valid_percentage():
    report = dp.profile(_columns(10))
    with pytest.raises(ValueError, match="baseline"):
        report.check(max_row_count_drop=10)
    with pytest.raises(ValueError, match="baseline"):
        report.check(max_row_count_growth=10)
    for bad in (-1, 100.5, float("nan"), "10", True):
        with pytest.raises(ValueError, match="max_row_count_drop"):
            report.check(baseline=report, max_row_count_drop=bad)  # ty: ignore[invalid-argument-type]
    for bad in (-1, float("nan"), float("inf"), "10", True):
        with pytest.raises(ValueError, match="max_row_count_growth threshold must be"):
            report.check(baseline=report, max_row_count_growth=bad)  # ty: ignore[invalid-argument-type]
    # A count can more than double, so growth has no upper end.
    assert report.check(baseline=report, max_row_count_growth=250).verdict == "pass"


def test_the_check_entrypoint_flags_and_policy_keys(tmp_path: Path):
    baseline = tmp_path / "baseline.json"
    dp.profile(_csv(100), format="csv").save(baseline)
    source = tmp_path / "today.csv"
    source.write_bytes(_csv(80))
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({"max_row_count_drop": 10}), encoding="utf-8")
    common = [str(source), "--baseline", str(baseline)]

    assert entrypoint.main([*common, "--max-row-drop", "10"]) == 1
    assert entrypoint.main([*common, "--policy", str(policy)]) == 1
    assert entrypoint.main([*common, "--max-row-drop", "25"]) == 0
    assert entrypoint.main([*common, "--max-row-growth", "10"]) == 0

    source.write_bytes(_csv(200))
    policy.write_text(json.dumps({"max_row_count_growth": 10}), encoding="utf-8")
    assert entrypoint.main([*common, "--max-row-growth", "10"]) == 1
    assert entrypoint.main([*common, "--policy", str(policy)]) == 1
    assert entrypoint.main([*common, "--max-row-growth", "150"]) == 0
