"""A fact the report does not record is not a clean result (#1306, #1240).

The Rust half is ``tests/findings_unrecorded_facts.rs``. Here: the gate on a
saved report whose quote state was removed, and ``findings()`` on a flat
summary that omits how the source was read.
"""

from __future__ import annotations

import json

import dataprof as dp


def _quote_verdict(report: dp.ProfileReport) -> str:
    return report.check(forbid_findings=["unterminated_quote"]).verdict


def _with_quote(report: dp.ProfileReport, value: bool | None) -> dp.ProfileReport:
    document = json.loads(report.to_json())
    if value is None:
        document["execution"].pop("unterminated_quote", None)
    else:
        document["execution"]["unterminated_quote"] = value
    return dp.ProfileReport.from_json(json.dumps(document))


def _skipped(result, code: str) -> list[str]:
    return [entry["reason"] for entry in result.not_evaluated if entry["code"] == code]


def test_an_unrecorded_quote_check_is_inconclusive_on_csv(tmp_path):
    path = tmp_path / "one.csv"
    path.write_text("id\n1\n")
    report = dp.profile(str(path))
    got = {value: _quote_verdict(_with_quote(report, value)) for value in (True, False, None)}
    assert got == {True: "fail", False: "pass", None: "inconclusive"}


def test_a_source_without_quoted_fields_passes(tmp_path):
    path = tmp_path / "one.jsonl"
    path.write_text('{"id": 1}\n')
    wrong = []
    for label, report in (
        ("jsonl file", dp.profile(str(path))),
        ("dict", dp.profile({"id": [1, 2]})),
    ):
        assert report.unterminated_quote is None, label
        loaded = dp.ProfileReport.from_json(report.to_json())
        for form, candidate in (("profiled", report), ("loaded", loaded)):
            verdict = _quote_verdict(candidate)
            skipped = _skipped(candidate.findings(), "unterminated_quote")
            if verdict != "pass" or skipped:
                wrong.append((label, form, verdict, skipped))
    assert wrong == []


def _summary_without(summary: dict, *keys: str) -> dp.ProfileReport:
    changed = {**summary, "execution": dict(summary["execution"])}
    for key in keys:
        changed["execution"].pop(key, None)
    return dp.ProfileReport.from_dict(changed)


def test_a_summary_missing_how_the_source_was_read_lists_partial_scan(tmp_path):
    # #1240's cases: a missing fact neither invents a partial scan nor rules
    # one out.
    base = dp.profile({"x": [1, 2, 3, 4]}).to_dict()
    wrong = []
    for key in ("sampling_applied", "source_exhausted"):
        result = _summary_without(base, key).findings()
        codes = [finding.code for finding in result.findings]
        skipped = _skipped(result, "partial_scan")
        if "partial_scan" in codes or skipped != ["unrecorded"]:
            wrong.append((key, codes, skipped))

    # Recorded either way, the summary is decided as it was.
    complete = dp.ProfileReport.from_dict(base).findings()
    if _skipped(complete, "partial_scan") or any(
        finding.code == "partial_scan" for finding in complete.findings
    ):
        wrong.append(("recorded, complete", complete.to_dict()))
    truncated = {**base, "execution": {**base["execution"], "source_exhausted": False}}
    result = dp.ProfileReport.from_dict(truncated).findings()
    if [f.evidence.get("reason") for f in result.findings if f.code == "partial_scan"] != [
        "truncated"
    ]:
        wrong.append(("recorded, truncated", result.to_dict()))
    sampled = {**base, "execution": {**base["execution"], "sampling_applied": True}}
    result = _summary_without(sampled, "source_exhausted").findings()
    if [f.evidence.get("reason") for f in result.findings if f.code == "partial_scan"] != [
        "sampled"
    ]:
        wrong.append(("sampled, exhaustion missing", result.to_dict()))
    assert wrong == []


def test_a_summary_lists_the_quote_check_only_where_it_could_apply(tmp_path):
    csv = tmp_path / "one.csv"
    csv.write_text("id\n1\n")
    jsonl = tmp_path / "one.jsonl"
    jsonl.write_text('{"id": 1}\n')
    wrong = []
    csv_summary = dp.profile(str(csv)).to_dict()
    assert csv_summary["execution"]["unterminated_quote"] is False
    cases = (
        ("csv", csv_summary, []),
        ("jsonl", dp.profile(str(jsonl)).to_dict(), []),
        ("dict", dp.profile({"id": [1]}).to_dict(), []),
    )
    for label, summary, expected in cases:
        result = dp.ProfileReport.from_dict(summary).findings()
        if _skipped(result, "unterminated_quote") != expected:
            wrong.append((label, result.not_evaluated))
    # A CSV summary that does not carry the quote state cannot say it was
    # clean; one that is not CSV needs none.
    for label, summary, expected in (
        ("csv without quote state", cases[0][1], ["unrecorded"]),
        ("jsonl without quote state", cases[1][1], []),
    ):
        result = _summary_without(summary, "unterminated_quote").findings()
        if _skipped(result, "unterminated_quote") != expected:
            wrong.append((label, result.not_evaluated))
    # A file summary written before 0.14 records no format, so the quote
    # check cannot be ruled out for it.
    legacy = {key: value for key, value in cases[1][1].items() if key != "source_format"}
    result = dp.ProfileReport.from_dict(legacy).findings()
    if _skipped(result, "unterminated_quote") != ["unrecorded"]:
        wrong.append(("file without source_format", result.not_evaluated))
    assert wrong == []


def test_a_versioned_summary_without_its_ragged_count_lists_ragged_rows(tmp_path):
    # Every versioned summary writes the count, so a missing one is not a
    # zero (#1306 review).
    path = tmp_path / "ragged.csv"
    path.write_text("a,b\n1,2\n3\n4,5\n")
    summary = dp.profile(str(path)).to_dict()
    assert summary["execution"]["ragged_row_count"] == 1
    recorded = dp.ProfileReport.from_dict(summary).findings()
    assert [f.code for f in recorded.findings if f.code == "ragged_rows"] == ["ragged_rows"]
    loaded = _summary_without(summary, "ragged_row_count")
    # Saving and reloading the summary must not turn the absence into a zero.
    wrong = []
    for label, report in (
        ("loaded", loaded),
        ("to_dict round trip", dp.ProfileReport.from_dict(loaded.to_dict())),
        ("to_json round trip", dp.ProfileReport.from_json(loaded.to_json())),
    ):
        result = report.findings()
        found = [f.code for f in result.findings if f.code == "ragged_rows"]
        if found or _skipped(result, "ragged_rows") != ["unrecorded"]:
            wrong.append((label, found, result.not_evaluated))
    assert wrong == []


def test_a_pre_versioning_summary_lists_ragged_rows(tmp_path):
    summary = dp.profile({"x": [1, 2]}).to_dict()
    legacy = {key: value for key, value in summary.items() if key != "schema_version"}
    legacy["execution"] = {
        key: value for key, value in summary["execution"].items() if key != "ragged_row_count"
    }
    result = dp.ProfileReport.from_dict(legacy).findings()
    assert _skipped(result, "ragged_rows") == ["unrecorded"]
    assert _skipped(dp.ProfileReport.from_dict(summary).findings(), "ragged_rows") == []


def test_a_versioned_report_without_its_ragged_count_is_inconclusive(tmp_path):
    # #1310 reproduction: a saved report with schema_version 1 that omits
    # execution.ragged_row_count must read back as None and unrecorded, leaving
    # forbid_findings=["ragged_rows"] inconclusive instead of passing.
    path = tmp_path / "ragged.csv"
    path.write_text("a,b\n1,2\n3\n4,5\n")
    report = dp.profile(str(path))
    assert type(report.ragged_row_count) is int
    assert report.ragged_row_count == 1
    assert report.check(forbid_findings=["ragged_rows"]).verdict == "fail"

    document = json.loads(report.to_json())
    assert document["execution"]["ragged_row_count"] == 1
    del document["execution"]["ragged_row_count"]

    loaded = dp.ProfileReport.from_json(json.dumps(document))
    assert loaded.ragged_row_count is None
    assert loaded.check(forbid_findings=["ragged_rows"]).verdict == "inconclusive"
    findings = loaded.findings()
    assert [f.code for f in findings.findings if f.code == "ragged_rows"] == []
    assert _skipped(findings, "ragged_rows") == ["unrecorded"]
    assert "ragged_row_count" not in json.loads(loaded.to_json())["execution"]
    assert "ragged_row_count" not in loaded.to_dict()["execution"]

    # Recorded 0 passes cleanly.
    document["execution"]["ragged_row_count"] = 0
    loaded_zero = dp.ProfileReport.from_json(json.dumps(document))
    assert loaded_zero.ragged_row_count == 0
    assert loaded_zero.check(forbid_findings=["ragged_rows"]).verdict == "pass"
    assert _skipped(loaded_zero.findings(), "ragged_rows") == []

    # Fresh clean report still returns int 0 and serializes it.
    clean_path = tmp_path / "clean.csv"
    clean_path.write_text("a,b\n1,2\n3,4\n")
    clean = dp.profile(str(clean_path))
    assert type(clean.ragged_row_count) is int
    assert clean.ragged_row_count == 0
    assert json.loads(clean.to_json())["execution"]["ragged_row_count"] == 0
    assert clean.to_dict()["execution"]["ragged_row_count"] == 0
