"""The findings contract, asserted against the shared fixture (#375).

``tests/fixtures/findings_parity.json`` states the inputs, profiling options,
thresholds, and the exact findings document for each case. The rules run in
Rust (#909); this file asserts what Python returns for them, on a native report
and on one loaded from a flat summary, and ``tests/findings_parity.rs`` asserts
the Rust API against the same file.

Every case is also derived from the report read back from its own JSON
document: findings are not stored, so a saved report has to reproduce them
from what it serialized.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import dataprof as dp
import pytest

FIXTURE = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "findings_parity.json"


def _fixture() -> dict[str, Any]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _profile(paths: dict[str, Path], options: dict[str, Any]) -> dp.ProfileReport:
    """Map the fixture's option names onto this layer's spelling of them."""
    kwargs: dict[str, Any] = {}
    for key in ("metrics", "columns", "identifier_columns"):
        if key in options:
            kwargs[key] = options[key]
    if "max_rows" in options:
        kwargs["stop_condition"] = dp.StopCondition.max_rows(options["max_rows"])
    return dp.profile_file(paths[options.get("input", "orders.csv")], **kwargs)


@pytest.fixture(scope="module")
def cases() -> list[dict[str, Any]]:
    loaded = _fixture()["cases"]
    assert loaded, "fixture states no case"
    return loaded


@pytest.fixture(scope="module")
def csv_paths(tmp_path_factory) -> dict[str, Path]:
    directory = tmp_path_factory.mktemp("findings")
    paths = {}
    for name, text in _fixture()["inputs"].items():
        path = directory / name
        path.write_text(text, encoding="utf-8")
        paths[name] = path
    return paths


def test_python_findings_match_the_shared_fixture(
    csv_paths: dict[str, Path], cases: list[dict[str, Any]]
):
    for case in cases:
        report = _profile(csv_paths, case["options"])
        result = report.findings(**case["policy"])
        assert result.to_dict() == case["expected"], f"{case['name']}: {case['why']}"

        restored = dp.ProfileReport.from_json(report.to_json())
        assert restored.findings(**case["policy"]).to_dict() == case["expected"], (
            f"{case['name']}: a report read back from its document derived different findings"
        )

        # A flat summary has no native report behind it: its facts reach the
        # Rust rules through its accessors (#909), and must read the same.
        summary = dp.ProfileReport.from_dict(report.to_dict())
        assert summary._native_report is None, case["name"]
        assert summary.findings(**case["policy"]).to_dict() == case["expected"], (
            f"{case['name']}: a report read back from its flat summary derived different findings"
        )


def test_every_rule_and_reason_is_covered(cases: list[dict[str, Any]]):
    """The fixture is the coverage claim, so state what it must contain.

    A parity fixture that quietly lost a rule would still pass on both sides
    while guarding nothing for it. Two reasons are absent: ``estimated`` and
    ``sampled`` need more rows than a fixture should carry, so the Rust unit
    tests and ``test_findings.py`` cover them instead. ``unrecorded`` comes
    from the capped CSV case, whose quote state was never checked (#1306).
    """
    codes = {finding["code"] for case in cases for finding in case["expected"]["findings"]}
    assert codes == {
        "all_null",
        "constant_column",
        "duplicate_rows",
        "future_dates",
        "key_duplicates",
        "locale_numbers",
        "mixed_types",
        "null_heavy",
        "partial_scan",
        "ragged_rows",
        "records_skipped",
        "sensitive_pattern",
        "temporal_order_violations",
        "unterminated_quote",
    }
    reasons = {entry["reason"] for case in cases for entry in case["expected"]["not_evaluated"]}
    assert reasons == {
        "quality_unavailable",
        "not_assessed",
        "not_computed",
        "no_values",
        "unrecorded",
    }
