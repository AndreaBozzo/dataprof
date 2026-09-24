"""``ProfileReport.findings()``: thresholds, absence, and the result object (#375).

The cross-language contract lives in ``test_findings_parity.py``; this file
covers what only the Python surface can be asked (keyword validation, the
result object's accessors) and the cases the fixture cannot reach cheaply,
built by editing a real report's document and reading it back.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import dataprof as dp
import pytest

CSV = (
    "order_id,email,channel,amount\n"
    "A-1,a@x.com,web,10\n"
    "A-2,b@y.com,web,20\n"
    "A-3,c@z.com,web,\n"
    "A-4,d@w.com,web,40\n"
)


@pytest.fixture
def report(tmp_path: Path) -> dp.ProfileReport:
    path = tmp_path / "orders.csv"
    path.write_text(CSV, encoding="utf-8")
    return dp.profile_file(path)


def _codes(result: dp.FindingsResult) -> list[tuple[str, str | None]]:
    return [(finding.code, finding.column) for finding in result]


def _edited(report: dp.ProfileReport, edit) -> dp.ProfileReport:
    document: dict[str, Any] = report.to_dict()
    edit(document)
    return dp.ProfileReport.from_dict(document)


def test_findings_are_typed_and_carry_no_raw_values(report: dp.ProfileReport):
    result = report.findings()

    assert _codes(result) == [
        ("null_heavy", "amount"),
        ("constant_column", "channel"),
        ("sensitive_pattern", "email"),
    ]
    for finding in result:
        assert isinstance(finding, dp.Finding)
        assert finding.severity == ("warning" if finding.code == "null_heavy" else "info")
    # The emails and order ids are in the report's columns; none may reach a
    # finding, whose evidence is metric values and names only.
    rendered = result.to_json()
    for raw in ("a@x.com", "A-1", "web"):
        assert raw not in rendered


def test_the_result_has_no_truthiness(report: dp.ProfileReport):
    """An empty findings list is clean only for rules not in ``not_evaluated``.

    A ``len()`` or ``bool()`` would collapse that to one number, so the result
    does not offer one: callers read the two fields.
    """
    result = report.findings()
    assert not hasattr(result, "__len__")
    # An object without __bool__ is truthy, which would make
    # `if not report.findings():` a branch that never runs.
    with pytest.raises(TypeError, match="no truth value"):
        bool(result)
    assert list(result) == list(result.findings)


def test_thresholds_are_validated_by_name(report: dp.ProfileReport):
    for value in (0, -5, 100.5, float("nan"), True, "twenty"):
        # Through a mapping, so the checker lets the wrong types reach the call.
        keywords: dict[str, Any] = {"null_heavy_percentage": value}
        with pytest.raises(ValueError, match="null_heavy_percentage must be a percentage"):
            report.findings(**keywords)
    with pytest.raises(ValueError, match="mixed_types_percentage must be a percentage"):
        report.findings(mixed_types_percentage=0)
    # The top of the range is accepted. Only a column whose null share rounds
    # to 100% qualifies there, and a fully null one is `all_null` instead.
    assert "null_heavy" not in {f.code for f in report.findings(null_heavy_percentage=100)}


def test_an_estimated_duplicate_count_witnesses_nothing(report: dp.ProfileReport):
    def estimated(document: dict[str, Any]) -> None:
        uniqueness = document["quality"]["uniqueness"]
        uniqueness["duplicate_rows"] = 3
        uniqueness["duplicate_rows_approximate"] = True

    result = _edited(report, estimated).findings()

    assert "duplicate_rows" not in {finding.code for finding in result}
    assert {"code": "duplicate_rows", "reason": "estimated"} in result.not_evaluated


def test_the_threshold_is_compared_at_the_serialized_precision(report: dp.ProfileReport):
    """4,999 of 25,000 is 19.996%, serialized as 20.0: reported at 20."""

    def near(document: dict[str, Any]) -> None:
        column = document["columns"][0]
        column["total_count"] = 25_000
        column["null_count"] = 4_999
        column["null_percentage"] = 20.0

    result = _edited(report, near).findings()

    finding = next(f for f in result if f.code == "null_heavy" and f.column == "order_id")
    assert finding.evidence["null_percentage"] == 20.0


def test_only_confident_personal_or_financial_patterns_are_sensitive(report: dp.ProfileReport):
    def patterns(document: dict[str, Any]) -> None:
        document["columns"][0]["patterns"] = [
            {"name": name, "regex": "", "match_count": 4, "match_percentage": 100.0,
             "category": category, "confidence": confidence}
            for name, category, confidence in (
                ("UUID", "identifier", 0.9),
                ("SSN (US)", "identifier", 0.9),
                ("IBAN", "financial", 0.4),
                ("Email", "contact", 0.8),
                ("IPv4", "network", 0.9),
            )
        ]  # fmt: skip

    result = _edited(report, patterns).findings()

    reported = [f.evidence["pattern"] for f in result if f.column == "order_id"]
    # Ordered by pattern name within the column, not by detection order.
    assert reported == ["Email", "SSN (US)"]
