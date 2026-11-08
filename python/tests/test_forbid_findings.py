"""A gate rule that fails on named findings (#1066).

``tests/forbid_findings.rs`` is the Rust twin; the gate's shared fixture
(``tests/fixtures/quality_gate_parity.json``) holds the partial-scan cases.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import dataprof as dp
import pytest

AMOUNTS = [f"{i}.50" for i in range(990)] + [f"{i},50" for i in range(10)]


@pytest.fixture
def source(tmp_path: Path) -> Path:
    path = tmp_path / "amounts.csv"
    rows = "".join(f'"{value}"\n' for value in AMOUNTS)
    path.write_text(f"amount\n{rows}", encoding="utf-8")
    return path


def _outcome(result) -> list[tuple[str | None, str, list[str | None]]]:
    return [
        (check.finding, check.status, [finding.get("column") for finding in check.findings])
        for check in result.checks
        if check.code == "forbid_finding"
    ]


def test_a_forbidden_finding_fails_the_gate_on_every_route(source: Path, tmp_path: Path):
    in_memory = dp.profile({"amount": AMOUNTS})
    # The gap the issue names: the score rule cannot see a 1% defect.
    assert in_memory.check(min_quality_score=95, max_null_percentage=0).verdict == "pass"

    from_file = dp.profile_file(source)
    saved = tmp_path / "report.json"
    from_file.save(saved)
    reloaded = dp.ProfileReport.load(saved)
    forbid = ["mixed_types", "locale_numbers"]
    routes = {
        "in-memory": in_memory.check(forbid_findings=forbid),
        "file": from_file.check(forbid_findings=forbid),
        "reloaded": reloaded.check(forbid_findings=forbid),
        "against a baseline": from_file.check(forbid_findings=forbid, baseline=reloaded),
    }

    # Codes come out in code order, whatever order the policy named them in.
    expected = [("locale_numbers", "failed", ["amount"]), ("mixed_types", "passed", [])]
    wrong = {
        route: (result.verdict, _outcome(result))
        for route, result in routes.items()
        if result.verdict != "fail" or _outcome(result) != expected
    }
    assert not wrong, f"routes that disagree: {wrong}"


def test_the_failed_check_carries_the_finding_evidence():
    result = dp.profile({"amount": AMOUNTS}).check(forbid_findings=["locale_numbers"])
    (check,) = result.violations
    (finding,) = check.findings
    assert finding["evidence"]["locale_number_count"] == 10
    assert check.observed == 1
    # The finding fields are fields of their own, not part of a reason.
    assert check.reason is None
    assert check.to_dict()["findings"] == [finding]


@pytest.mark.parametrize(
    "value",
    ["locale_numbers", ["nope"], [1], {"locale_numbers": True}],
    ids=["bare-string", "unknown-code", "not-a-name", "mapping"],
)
def test_an_unusable_forbid_findings_is_a_policy_error(value):
    report = dp.profile({"amount": AMOUNTS})
    with pytest.raises(ValueError, match="forbid_findings"):
        report.check(forbid_findings=value)


def run_check(*args: str | Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "dataprof.check", *(str(arg) for arg in args)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=60,
        check=False,
    )


def test_the_command_line_fails_on_a_forbidden_finding(source: Path):
    result = run_check(source, "--forbid-finding", "locale_numbers", "--json")
    assert result.returncode == 1, result.stderr
    expected = dp.profile_file(source).check(forbid_findings=["locale_numbers"])
    assert json.loads(result.stdout) == expected.to_dict()
    assert "failed: forbid_finding [locale_numbers]" in result.stderr


def test_the_command_line_passes_when_the_rule_found_nothing(source: Path):
    result = run_check(source, "--forbid-finding", "records_skipped")
    assert result.returncode == 0, result.stderr


def test_an_unknown_code_on_the_command_line_is_a_policy_error(source: Path):
    result = run_check(source, "--forbid-finding", "nope", "--json")
    assert result.returncode == 3, result.stderr
    error = json.loads(result.stdout)["error"]
    assert error["kind"] == "policy"
    assert "nope" in error["message"]
    assert "path" not in error


def test_a_policy_file_forbids_findings(source: Path, tmp_path: Path):
    path = tmp_path / "policy.json"
    path.write_text(json.dumps({"forbid_findings": ["locale_numbers"]}), encoding="utf-8")
    result = run_check(source, "--policy", path)
    assert result.returncode == 1, result.stderr
