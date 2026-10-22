"""Baseline-relative quality-gate rules through ProfileReport.check() (#749).

`tests/baseline_gate.rs` is the Rust twin. Here the extra routes are the Python
ones: a baseline saved with ``save()`` and one loaded from a flat ``to_dict()``
summary, including one written by 0.11.
"""

import json
from pathlib import Path
from typing import Any

import dataprof as dp
import pytest

FIXTURES = Path(__file__).parents[2] / "tests/fixtures"
CLEAN = {"id": [1, 2, 3, 4], "amount": [10, 20, 30, 40]}
WITH_NULLS = {"id": [1, 2, 3, 4], "amount": [10, None, None, 40]}
DRIFT: dict[str, Any] = {
    "max_quality_score_drop": 0,
    "max_dimension_score_drops": {"completeness": 0},
    "max_null_percentage_increase": 0,
}


def _find(result, code, subject=None):
    for check in result.checks:
        if check.code == code and subject in (None, check.column, check.dimension):
            return check
    raise AssertionError(f"no {code} check for {subject}")


def _id(report: dp.ProfileReport) -> str:
    return json.loads(report.to_json())["id"]


def _reason(check):
    return None if check.reason is None else check.reason["reason"]


def test_unchanged_data_passes_every_change_rule():
    baseline = dp.profile(CLEAN)
    report = dp.profile(CLEAN)
    result = report.check(baseline=baseline, **DRIFT)

    assert result.verdict == "pass", result.to_dict()
    assert [(check.code, check.column) for check in result.checks] == [
        ("max_quality_score_drop", None),
        ("max_dimension_score_drop", None),
        ("max_null_percentage_increase", "id"),
        ("max_null_percentage_increase", "amount"),
    ]
    assert result.baseline is not None
    assert result.baseline["reports"]["a"]["id"] == _id(baseline)
    assert result.baseline["reports"]["b"]["id"] == _id(report)


def test_a_degradation_fails_and_an_improvement_passes():
    clean = dp.profile(CLEAN)
    nulls = dp.profile(WITH_NULLS)
    policy: dict[str, Any] = {
        "max_dimension_score_drops": {"completeness": 5},
        "max_null_percentage_increase": {"amount": 10},
    }

    worse = nulls.check(baseline=clean, **policy)
    assert worse.verdict == "fail"
    assert _find(worse, "max_dimension_score_drop", "completeness").is_violation
    amount = _find(worse, "max_null_percentage_increase", "amount")
    assert amount.is_violation
    assert amount.observed == 50.0
    assert amount.baseline == {
        "observed": 0.0,
        "evidence": {"coverage": "complete"},
        "change": 50.0,
    }
    assert amount.expected == {"comparison": "change_at_most", "value": 10.0}

    assert clean.check(baseline=nulls, **policy).verdict == "pass"


def test_a_change_rule_needs_a_baseline_a_percentage_and_a_report():
    report = dp.profile(CLEAN)
    # Refused by the keyword validator, which names the rules, before the
    # Rust gate is reached.
    with pytest.raises(ValueError, match=r"rule about change \(max_quality_score_drop"):
        report.check(**DRIFT)
    with pytest.raises(ValueError, match="max_null_percentage_increase threshold for `amount`"):
        report.check(baseline=report, max_null_percentage_increase={"amount": 101})
    with pytest.raises(ValueError, match="unknown quality dimension"):
        report.check(baseline=report, max_dimension_score_drops={"freshness": 1})
    with pytest.raises(TypeError, match="baseline must be a dataprof ProfileReport"):
        report.check(baseline=report.to_dict(), max_quality_score_drop=1)  # ty: ignore[invalid-argument-type]
    # A baseline alone is not a requirement.
    with pytest.raises(ValueError, match="states no requirement"):
        report.check(baseline=report)


def test_absolute_and_change_rules_evaluate_together_absolute_first():
    result = dp.profile(WITH_NULLS).check(
        baseline=dp.profile(CLEAN),
        min_rows=1,
        max_null_percentage={"amount": 100},
        max_null_percentage_increase={"amount": 0},
    )
    assert [check.code for check in result.checks] == [
        "min_rows",
        "max_null_percentage",
        "max_null_percentage_increase",
    ]
    assert [check.baseline is not None for check in result.checks] == [False, False, True]
    assert result.verdict == "fail"


def test_a_changed_column_set_leaves_dataset_rules_unevaluated_and_column_rules_decisive():
    baseline = dp.profile(CLEAN)
    added = dp.profile({**CLEAN, "note": [None, None, None, "x"]})
    result = added.check(
        baseline=baseline,
        max_quality_score_drop=0,
        max_dimension_score_drops={"completeness": 0},
        max_null_percentage_increase={"*": 0, "note": 0},
    )

    assert _reason(_find(result, "max_quality_score_drop")) == "schema_changed"
    assert _reason(_find(result, "max_dimension_score_drop")) == "schema_changed"
    # The new column has no baseline: only the rule that names it reads it.
    assert _reason(_find(result, "max_null_percentage_increase", "note")) == "not_in_baseline"
    assert [
        check.column for check in result.checks if check.code == "max_null_percentage_increase"
    ] == ["note", "id", "amount"]
    assert result.verdict == "inconclusive"
    assert result.baseline is not None
    assert result.baseline["schema"]["added"] == ["note"]

    # A shared column's rule still decides: a failure beats the unevaluated
    # dataset-level rules.
    worse = dp.profile({**WITH_NULLS, "note": [None, None, None, "x"]})
    result = worse.check(baseline=baseline, **DRIFT)
    assert result.verdict == "fail"
    # "*" covers the shared columns only; the unnamed new one gets no rule.
    assert [check.column for check in result.checks if check.column] == ["id", "amount"]
    assert _find(result, "max_null_percentage_increase", "amount").is_violation

    removed = dp.profile({"id": [1, 2, 3, 4]})
    result = removed.check(baseline=baseline, max_null_percentage_increase={"amount": 0})
    assert _reason(result.checks[0]) == "column_not_profiled"


def test_a_score_either_side_lacks_is_unevaluated():
    undated = dp.profile({"d": ["abc", "xyz"]})
    dated = dp.profile({"d": ["2026-01-01", "2026-01-02"]})
    policy: dict[str, Any] = {"max_dimension_score_drops": {"timeliness": 0}}

    assert _reason(dated.check(baseline=undated, **policy).checks[0]) == "not_in_baseline"
    assert _reason(undated.check(baseline=undated, **policy).checks[0]) == "not_assessed"


def test_a_setting_leaves_only_the_scores_it_moves_unevaluated():
    data = {"v": [-3, -1, 2, 4]}
    plain = dp.profile(data)
    hinted = dp.profile(data, positive_columns=["v"])
    result = hinted.check(
        baseline=plain,
        max_quality_score_drop=0,
        max_dimension_score_drops={"accuracy": 0, "completeness": 0},
    )

    for check in (
        _find(result, "max_quality_score_drop"),
        _find(result, "max_dimension_score_drop", "accuracy"),
    ):
        assert check.reason == {"reason": "setting_changed", "setting": "positive_columns"}
    assert _find(result, "max_dimension_score_drop", "completeness").status == "passed"

    # Timeliness reads the temporal hints.
    dates = {"d": ["2026-01-01", "2026-01-02"]}
    result = dp.profile(dates, temporal_columns=["d"]).check(
        baseline=dp.profile(dates),
        max_dimension_score_drops={"timeliness": 0, "completeness": 0},
    )
    assert _find(result, "max_dimension_score_drop", "timeliness").reason == {
        "reason": "setting_changed",
        "setting": "temporal_columns",
    }
    assert _find(result, "max_dimension_score_drop", "completeness").status == "passed"


def test_a_baseline_that_predates_a_record_leaves_what_depends_on_it_unevaluated(
    tmp_path: Path,
):
    report = dp.profile(CLEAN)
    summary = report.to_dict()
    # A summary from before 0.10 records no version, and so no hints.
    del summary["schema_version"]
    result = report.check(
        baseline=dp.ProfileReport.from_dict(summary),
        max_dimension_score_drops={"accuracy": 0, "completeness": 0},
    )
    assert _find(result, "max_dimension_score_drop", "accuracy").reason == {
        "reason": "setting_unrecorded",
        "setting": "identifier_columns",
    }
    assert _find(result, "max_dimension_score_drop", "completeness").status == "passed"

    # A 0.11 summary records no measurement definitions.
    text = (FIXTURES / "legacy_0_11_non_ascii.summary.json").read_text(encoding="utf-8")
    legacy = dp.ProfileReport.from_dict(json.loads(text))
    path = tmp_path / "cities.csv"
    path.write_text("city\n東京\nZürich\nOslo\n\n", encoding="utf-8")
    result = dp.profile(str(path)).check(
        baseline=legacy, max_quality_score_drop=0, max_null_percentage_increase=0
    )
    for check in result.checks:
        assert check.reason == {
            "reason": "definition_not_comparable",
            "definition": "null_tokens",
        }, check.to_dict()
    assert result.verdict == "inconclusive"


def test_a_baseline_without_the_range_rule_definition_leaves_accuracy_unevaluated():
    # Every other definition recorded, so only the range rule count separates
    # the baseline from the report (#1005).
    report = dp.profile(CLEAN)
    summary = report.to_dict()
    del summary["metric_semantics"]["range_rule_count"]
    result = report.check(
        baseline=dp.ProfileReport.from_dict(summary),
        max_quality_score_drop=0,
        max_dimension_score_drops={"accuracy": 0, "completeness": 0},
    )
    unevaluated = {"reason": "definition_not_comparable", "definition": "range_rule_count"}
    assert _find(result, "max_quality_score_drop").reason == unevaluated
    assert _find(result, "max_dimension_score_drop", "accuracy").reason == unevaluated
    assert _find(result, "max_dimension_score_drop", "completeness").status == "passed"


def test_partial_evidence_on_either_side_is_unevaluated_under_full_source_only(tmp_path: Path):
    path = tmp_path / "rows.csv"
    path.write_text("id,amount\n" + "".join(f"{i},{i * 2}\n" for i in range(200)))
    exact = dp.profile(str(path))
    truncated = dp.profile(str(path), max_rows=50)
    policy: dict[str, Any] = {
        "max_dimension_score_drops": {"completeness": 100},
        "max_null_percentage_increase": 0,
    }

    for report, baseline in ((truncated, exact), (exact, truncated)):
        result = report.check(baseline=baseline, **policy)
        for check in result.checks:
            assert check.reason == {"reason": "evidence_incomplete", "gap": "truncated"}
        assert result.verdict == "inconclusive"
        assert report.check(baseline=baseline, scope="observed", **policy).verdict == "pass"


def test_a_saved_baseline_gives_the_verdict_the_live_one_does(tmp_path: Path):
    baseline = dp.profile(WITH_NULLS)
    report = dp.profile(CLEAN)
    policy: dict[str, Any] = {
        **DRIFT,
        "max_dimension_score_drops": {"completeness": 0, "uniqueness": 0},
    }
    live = report.check(baseline=baseline, **policy).to_dict()

    saved = tmp_path / "baseline.json"
    baseline.save(saved)
    assert report.check(baseline=dp.ProfileReport.load(saved), **policy).to_dict() == live

    flat = report.check(baseline=dp.ProfileReport.from_dict(baseline.to_dict()), **policy)
    document = flat.to_dict()
    # A flat summary records neither identity field.
    assert document["baseline"]["reports"]["a"]["id"] is None
    document["baseline"]["reports"]["a"]["id"] = _id(baseline)
    document["baseline"]["reports"]["a"]["timestamp"] = live["baseline"]["reports"]["a"][
        "timestamp"
    ]
    assert document == live


def test_the_document_keeps_the_absolute_shape_and_adds_the_baseline():
    report = dp.profile(CLEAN)
    absolute = report.check(min_rows=1)
    assert absolute.baseline is None
    assert "baseline" not in absolute.to_dict()
    assert "baseline" not in absolute.to_dict()["checks"][0]

    relative = report.check(baseline=report, **DRIFT)
    document = relative.to_dict()
    assert json.loads(relative.to_json()) == document
    assert list(document) == ["verdict", "scope", "evidence", "checks", "baseline"]
    assert document["baseline"]["schema"] == {
        "added": [],
        "removed": [],
        "common": ["id", "amount"],
    }
    # A permitted drop of zero is 0.0, not -0.0.
    assert json.dumps(document["checks"][0]["expected"]) == (
        '{"comparison": "change_at_least", "value": 0.0}'
    )
    # The completeness rule, which is decided.
    assert list(document["checks"][1]) == [
        "code",
        "dimension",
        "expected",
        "observed",
        "scope",
        "evidence",
        "baseline",
        "status",
        "message",
    ]


def test_the_overall_score_needs_the_same_assessed_dimensions():
    # Same column, but only the dated side has anything for timeliness.
    undated = dp.profile({"d": ["abc", "xyz"]})
    dated = dp.profile({"d": ["2026-01-01", "2026-01-02"]})
    result = dated.check(baseline=undated, max_quality_score_drop=100)
    assert result.checks[0].reason == {"reason": "dimensions_changed"}


def test_order_and_pairing_the_reports_cannot_show_are_not_assumed_the_same():
    keys = {"a": [1, 2, 3], "b": [1, 1, 2]}
    policy: dict[str, Any] = {"max_dimension_score_drops": {"uniqueness": 100}}

    # The first identifier present is the uniqueness key, so order matters.
    reordered = dp.profile(keys, identifier_columns=["b", "a"]).check(
        baseline=dp.profile(keys, identifier_columns=["a", "b"]), **policy
    )
    assert reordered.checks[0].reason == {
        "reason": "setting_changed",
        "setting": "identifier_columns",
    }
    same = dp.profile(keys, identifier_columns=["a", "b"]).check(
        baseline=dp.profile(keys, identifier_columns=["a", "b"]), **policy
    )
    assert same.checks[0].status == "passed"

    # A report records which columns the pairs named, not the pairing.
    dates = {"start": ["2026-01-01", "2026-01-03"], "end": ["2026-01-02", "2026-01-04"]}
    policy = {"max_dimension_score_drops": {"timeliness": 100}}
    paired = dp.profile(dates, temporal_pairs=[("start", "end")])
    assert paired.check(baseline=paired, **policy).checks[0].reason == {
        "reason": "setting_unrecorded",
        "setting": "temporal_pairs",
    }
    unpaired = dp.profile(dates)
    assert unpaired.check(baseline=unpaired, **policy).checks[0].status == "passed"

    # Validity reads the pattern locale, which both reports record (#1010);
    # test_pattern_locale_record.py covers a locale that differs.
    emails = dp.profile({"email": ["a@example.com", "b@example.com"]})
    result = emails.check(baseline=emails, max_dimension_score_drops={"validity": 100})
    assert result.checks[0].status == "passed"


def test_an_allowance_is_compared_at_the_serialized_precision():
    # One null in 10,000 values is a change of exactly 0.01 points.
    values = list(range(10_000))
    baseline = dp.profile({"v": values})
    with_null = dp.profile({"v": [None, *values[1:]]})

    def decide(points: float) -> list[dp.QualityCheck]:
        return list(
            with_null.check(
                baseline=baseline,
                max_dimension_score_drops={"completeness": points},
                max_null_percentage_increase={"v": points},
            ).checks
        )

    # 0.006 serializes as 0.01, the change it is compared with; completeness
    # drops by the same 0.01.
    for check in decide(0.006):
        assert check.status == "passed", check.to_dict()
        assert check.baseline is not None
        assert abs(check.baseline["change"]) == 0.01
        assert abs(check.expected["value"]) == 0.01
    for check in decide(0.004):
        assert check.status == "failed", check.to_dict()
        assert abs(check.expected["value"]) == 0.0


BEFORE = {"id": [1, 2], "amount": [10, 20], "code": ["a", "b"], "gone": ["x", "y"]}
AFTER = {"id": [1, 2], "amount": ["x1", "y2"], "code": [None, None], "new": ["z", "w"]}


def _listed(result) -> list[dict[str, Any]]:
    assert result.baseline is not None
    return result.baseline["diagnostics"]


def _diagnostics(result):
    return [(item["code"], item["column"]) for item in _listed(result)]


def test_a_schema_change_is_a_diagnostic_and_moves_no_verdict(tmp_path: Path):
    baseline = dp.profile(BEFORE)
    report = dp.profile(AFTER)
    policy: dict[str, Any] = {"max_null_percentage_increase": {"id": 0}}
    result = report.check(baseline=baseline, **policy)

    assert result.verdict == "pass", result.to_dict()
    assert _diagnostics(result) == [
        ("column_type_changed", "amount"),
        ("column_all_null", "code"),
        ("column_removed", "gone"),
        ("column_added", "new"),
    ]
    assert _listed(result)[0] == {
        "code": "column_type_changed",
        "column": "amount",
        "data_type": {"a": "integer", "b": "string"},
        "message": "this column holds values on both sides, and its inferred type differs",
    }
    assert list(_listed(result)[1]) == ["code", "column", "message"]
    assert _listed(report.check(baseline=report, **policy)) == []

    # Saved, or flattened to a summary, the baseline gives the same ones.
    saved = tmp_path / "baseline.json"
    baseline.save(saved)
    loaded = dp.ProfileReport.load(saved)
    flat = dp.ProfileReport.from_dict(baseline.to_dict())
    for other in (loaded, flat):
        assert _listed(report.check(baseline=other, **policy)) == _listed(result)


def test_an_all_null_side_establishes_no_type():
    empty = dp.profile({"id": [1, 2], "v": [None, None]})
    dated = dp.profile({"id": [1, 2], "v": ["2026-01-01", "2026-01-02"]})
    assert _diagnostics(dated.check(baseline=empty, **DRIFT)) == []
    assert _diagnostics(empty.check(baseline=dated, **DRIFT)) == [("column_all_null", "v")]


def test_a_0_11_summary_baseline_reports_schema_changes():
    # Its one column, `city`, is a string with values; 0.11 recorded the
    # type and the counts the diagnostics read.
    legacy = dp.ProfileReport.from_dict(
        json.loads((FIXTURES / "legacy_0_11_non_ascii.summary.json").read_text("utf-8"))
    )
    emptied = dp.profile({"city": [None, None], "extra": [1, 2]})
    assert _diagnostics(emptied.check(baseline=legacy, max_null_percentage_increase=100)) == [
        ("column_all_null", "city"),
        ("column_added", "extra"),
    ]
    retyped = dp.profile({"city": [1, 2]})
    result = retyped.check(baseline=legacy, max_null_percentage_increase=100)
    assert _listed(result)[0]["data_type"] == {"a": "string", "b": "integer"}


def test_a_type_change_the_identifier_hint_explains_names_the_hint():
    data = {"id": [1, 2], "n": [5, 6]}
    plain = dp.profile(data)
    hinted = dp.profile(data, identifier_columns=["id"])
    result = hinted.check(baseline=plain, max_null_percentage_increase={"n": 0})

    assert result.verdict == "pass"
    [retyped] = _listed(result)
    assert retyped["data_type"] == {"a": "integer", "b": "identifier"}
    assert (retyped["reason"], retyped["setting"]) == ("setting_changed", "identifier_columns")
    # A data-driven change carries no reason.
    assert "reason" not in _listed(dp.profile(AFTER).check(baseline=dp.profile(BEFORE), **DRIFT))[0]
