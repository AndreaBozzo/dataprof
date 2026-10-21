"""Rules about the column set through ProfileReport.check() (#749).

``tests/schema_rules.rs`` is the Rust twin. Here the extra routes are the
Python ones: in-memory inputs, ``save()``/``load()`` and a flat ``to_dict()``
summary baseline.
"""

import json
from pathlib import Path
from typing import Any

import dataprof as dp
import pytest

ORDERS = {"id": [1, 2, 3], "amount": [10, 20, 30], "note": ["a", "b", "c"]}
NO_NOTE = {"id": [1, 2, 3], "amount": [10, 20, 30]}


def _outcomes(result) -> list[tuple[str, str, str]]:
    outcomes = []
    for check in result.checks:
        if check.status != "not_evaluated":
            outcome = check.status
        else:
            reason = check.reason["reason"]
            detail = check.reason.get("setting") or check.reason.get("gap")
            outcome = f"{reason}:{detail}" if detail else reason
        outcomes.append((check.code, check.column, outcome))
    return outcomes


def _without_settings(report: dp.ProfileReport) -> dp.ProfileReport:
    document = json.loads(report.to_json())
    del document["analysis_settings"]
    return dp.ProfileReport.from_dict(document)


def test_a_required_column_fails_only_when_the_report_shows_its_source_lacks_it(tmp_path: Path):
    policy: dict[str, Any] = {"require_columns": ["note", "id"]}
    assert _outcomes(dp.profile(ORDERS).check(**policy)) == [
        ("require_column", "id", "passed"),
        ("require_column", "note", "passed"),
    ]
    missing = dp.profile(NO_NOTE)
    assert _outcomes(missing.check(**policy))[1] == ("require_column", "note", "failed")
    # A CSV header lists every column however few rows were read.
    csv = tmp_path / "no_note.csv"
    csv.write_text("id,amount\n1,10\n2,20\n3,30\n")
    truncated = dp.profile(str(csv), max_rows=1)
    assert _outcomes(truncated.check(**policy))[1] == ("require_column", "note", "failed")
    projected = dp.profile(ORDERS, columns=["id", "amount"])
    assert _outcomes(projected.check(**policy))[1] == (
        "require_column",
        "note",
        "column_not_profiled",
    )
    assert _outcomes(_without_settings(missing).check(**policy))[1] == (
        "require_column",
        "note",
        "setting_unrecorded:columns",
    )


def test_a_jsonl_column_set_covers_the_source_only_when_every_record_was_read(tmp_path: Path):
    records = tmp_path / "late.jsonl"
    records.write_text('{"id": 1}\n{"id": 2}\n{"id": 3, "note": "a"}\n')
    truncated = dp.profile(str(records), max_rows=1)
    assert _outcomes(truncated.check(require_columns=["note"])) == [
        ("require_column", "note", "evidence_incomplete:truncated")
    ]
    assert _outcomes(truncated.check(require_columns=["note"], scope="observed")) == [
        ("require_column", "note", "failed")
    ]


def test_removed_and_added_columns_fail_against_a_baseline_that_shows_the_change(
    tmp_path: Path,
):
    baseline = dp.profile(ORDERS)
    policy: dict[str, Any] = {"forbid_removed_columns": True, "forbid_added_columns": True}
    lost = dp.profile(NO_NOTE).check(baseline=baseline, **policy)
    assert lost.verdict == "fail"
    assert _outcomes(lost) == [
        ("forbid_removed_column", "id", "passed"),
        ("forbid_removed_column", "amount", "passed"),
        ("forbid_removed_column", "note", "failed"),
        ("forbid_added_column", "id", "passed"),
        ("forbid_added_column", "amount", "passed"),
    ]
    assert lost.baseline is not None and len(lost.baseline["diagnostics"]) == 1

    projected = dp.profile(ORDERS, columns=["id", "amount"])
    assert ("forbid_removed_column", "note", "setting_changed:columns") in _outcomes(
        projected.check(baseline=baseline, **policy)
    )
    # Saved, or flattened to a summary, the baseline decides alike: an
    # in-memory source declares its columns, and a summary records that.
    saved = tmp_path / "baseline.json"
    dp.profile(NO_NOTE).save(saved)
    for other in (
        dp.ProfileReport.load(saved),
        dp.ProfileReport.from_dict(dp.profile(NO_NOTE).to_dict()),
    ):
        assert ("forbid_added_column", "note", "failed") in _outcomes(
            baseline.check(baseline=other, **policy)
        )


def test_the_keywords_are_validated():
    report = dp.profile(ORDERS)
    with pytest.raises(ValueError, match="require_columns must be a list"):
        report.check(require_columns="id")
    with pytest.raises(ValueError, match="require_columns must be a list"):
        numbers: dict[str, Any] = {"require_columns": [1]}
        report.check(**numbers)
    with pytest.raises(ValueError, match="forbid_added_columns must be True or False"):
        bad: dict[str, Any] = {"forbid_added_columns": "yes"}
        report.check(baseline=report, **bad)
    with pytest.raises(ValueError, match="needs a baseline"):
        report.check(forbid_removed_columns=True)
    document = report.check(require_columns=["id"], forbid_added_columns=True, baseline=report)
    assert document.to_dict()["checks"][0] == {
        "code": "require_column",
        "column": "id",
        "expected": {"comparison": "present"},
        "scope": "full_source",
        "evidence": {"coverage": "complete"},
        "status": "passed",
        "message": "the required column was profiled",
    }
