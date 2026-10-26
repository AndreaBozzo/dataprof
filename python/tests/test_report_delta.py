"""ProfileReport.compare() returns the Rust report delta (#713).

`tests/report_delta.rs` is the Rust twin. Here the extra routes are the
Python ones: a report loaded from a flat `to_dict()` summary, including a
summary written by 0.11, compares through the same delta as a profiled one.
"""

import json
from pathlib import Path

import dataprof as dp
import pytest

FIXTURES = Path(__file__).parents[2] / "tests/fixtures"
CLEAN = {"id": [1, 2, 3, 4], "amount": [10, 20, 30, 40]}
WITH_NULLS = {"id": [1, 2, 3, 4], "amount": [10, None, None, 40]}
SCORE_KEYS = {"a", "b", "abs", "rel_pct", "evidence"}


def _flat(report: dp.ProfileReport) -> dp.ProfileReport:
    return dp.ProfileReport.from_dict(report.to_dict())


def test_the_shape_is_the_frozen_contract():
    delta = dp.profile(WITH_NULLS).compare(dp.profile(CLEAN))
    assert list(delta) == [
        "quality_score",
        "dimensions",
        "columns",
        "metric_semantics",
        "configuration",
        "schema",
        "reports",
    ]
    assert set(delta["quality_score"]) == SCORE_KEYS
    assert list(delta["dimensions"]) == [
        "completeness",
        "consistency",
        "uniqueness",
        "accuracy",
        "timeliness",
        "validity",
        "precision",
    ]
    for change in delta["dimensions"].values():
        assert set(change) == SCORE_KEYS
        assert set(change["evidence"]) == {"a", "b"}
    assert delta["columns"]["amount"] == {
        "null_pct_a": 50.0,
        "null_pct_b": 0.0,
        "null_pct_delta": -50.0,
        "data_type_a": "integer",
        "data_type_b": "integer",
    }
    assert set(delta["metric_semantics"]) == {"a", "b", "comparable"}
    assert list(delta["configuration"]) == [
        "score_weights",
        "semantic_hints",
        "pattern_locale",
        "columns",
    ]
    for setting in delta["configuration"].values():
        assert set(setting) == {"a", "b", "same"}
    assert set(delta["configuration"]["semantic_hints"]["a"]) == {
        "positive_columns",
        "identifier_columns",
        "temporal_columns",
        "temporal_pair_columns",
    }
    assert set(delta["schema"]) == {"added", "removed", "common"}
    for side in delta["reports"].values():
        assert set(side) == {"id", "timestamp", "source", "rows_processed", "evidence"}


def test_every_change_is_b_minus_a():
    delta = dp.profile(WITH_NULLS).compare(dp.profile(CLEAN))
    completeness = delta["dimensions"]["completeness"]
    assert completeness["b"] > completeness["a"]
    assert completeness["abs"] == round(completeness["b"] - completeness["a"], 2)
    assert completeness["abs"] > 0
    assert delta["columns"]["amount"]["null_pct_delta"] < 0


@pytest.mark.parametrize("flat", [False, True])
def test_column_types_cover_changed_added_and_removed_columns(flat):
    a = dp.profile({"value": ["hello", "world"], "old": [1, 2]})
    b = dp.profile({"value": [10, 20], "new": [True, False]})
    if flat:
        a, b = _flat(a), _flat(b)
    columns = a.compare(b)["columns"]
    assert columns["value"]["data_type_a"] == "string"
    assert columns["value"]["data_type_b"] == "integer"
    assert columns["old"]["data_type_a"] == "integer"
    assert columns["old"]["data_type_b"] is None
    assert columns["new"]["data_type_a"] is None
    assert columns["new"]["data_type_b"] == "boolean"


@pytest.mark.parametrize("suffix", ["csv", "jsonl"])
@pytest.mark.parametrize("flat", [False, True])
def test_all_null_columns_do_not_claim_an_inferred_type(tmp_path, suffix, flat):
    path = tmp_path / f"empty.{suffix}"
    path.write_text(
        "id,value\n1,\n2,\n"
        if suffix == "csv"
        else '{"id":1,"value":null}\n{"id":2,"value":null}\n',
        encoding="utf-8",
    )
    empty = dp.profile(path)
    filled = dp.profile({"id": [1, 2], "value": [10, 20]})
    if flat:
        empty, filled = _flat(empty), _flat(filled)
    change = empty.compare(filled)["columns"]["value"]
    assert change["null_pct_a"] == 100.0
    assert change["data_type_a"] is None
    assert change["data_type_b"] == "integer"
    reversed_change = filled.compare(empty)["columns"]["value"]
    assert reversed_change["data_type_a"] == "integer"
    assert reversed_change["data_type_b"] is None


@pytest.mark.parametrize("flat", [False, True])
def test_column_selection_compares_all_listed_and_legacy_metadata(flat):
    all_columns = dp.profile(CLEAN)
    selected = dp.profile(CLEAN, columns=["amount"])
    if flat:
        all_columns, selected = _flat(all_columns), _flat(selected)
    assert all_columns.compare(selected)["configuration"]["columns"] == {
        "a": {"selection": "all"},
        "b": {"selection": "listed", "names": ["amount"]},
        "same": False,
    }
    for report in (all_columns, selected):
        assert report.compare(report)["configuration"]["columns"]["same"] is True
    summary = all_columns.to_dict()
    del summary["analysis_settings"]
    legacy = dp.ProfileReport.from_dict(summary)
    assert legacy.compare(selected)["configuration"]["columns"] == {
        "a": None,
        "b": {"selection": "listed", "names": ["amount"]},
        "same": None,
    }


def test_legacy_column_type_metadata_is_not_invented():
    report = dp.profile(CLEAN)
    summary = report.to_dict()
    del summary["columns"][0]["data_type"]
    legacy = dp.ProfileReport.from_dict(summary)
    change = legacy.compare(report)["columns"]["id"]
    assert change["data_type_a"] is None
    assert change["data_type_b"] == "integer"


def test_a_flat_summary_compares_like_the_report_it_came_from():
    a, b = dp.profile(WITH_NULLS), dp.profile(CLEAN)
    native = a.compare(b)
    flat = _flat(a).compare(_flat(b))
    mixed = _flat(a).compare(b)

    assert native["reports"]["a"]["id"] == json.loads(a.to_json())["id"]
    for delta in (flat, mixed):
        assert delta["reports"]["a"]["id"] is None
        assert delta["reports"]["a"]["timestamp"] is None
    masked = json.loads(json.dumps(native))
    for side in ("a", "b"):
        masked["reports"][side]["id"] = None
        masked["reports"][side]["timestamp"] = None
    assert flat == masked
    masked["reports"]["b"] = native["reports"]["b"]
    assert mixed == masked


def test_identity_accessors_match_delta_for_native_reloaded_and_flat(tmp_path: Path):
    report = dp.profile(CLEAN)
    canonical = json.loads(report.to_json())
    assert report.id == canonical["id"]
    assert report.timestamp == canonical["timestamp"]

    saved = tmp_path / "identity.json"
    report.save(saved)
    restored = [
        report,
        dp.ProfileReport.from_json(report.to_json()),
        dp.ProfileReport.load(saved),
        _flat(report),
    ]
    for candidate in restored:
        delta = candidate.compare(candidate)
        expected_id = None if candidate is restored[-1] else canonical["id"]
        expected_timestamp = None if candidate is restored[-1] else canonical["timestamp"]
        assert candidate.id == expected_id
        assert candidate.timestamp == expected_timestamp
        for side in ("a", "b"):
            assert delta["reports"][side]["id"] == candidate.id
            assert delta["reports"][side]["timestamp"] == candidate.timestamp

    # These are properties, not mutable aliases for persisted document fields.
    with pytest.raises(AttributeError):
        setattr(report, "id", "forged")
    with pytest.raises(AttributeError):
        setattr(report, "timestamp", "2020-01-01T00:00:00Z")


def test_a_0_11_baseline_reads_as_unrecorded(tmp_path: Path):
    text = (FIXTURES / "legacy_0_11_non_ascii.summary.json").read_text(encoding="utf-8")
    baseline = dp.ProfileReport.from_dict(json.loads(text))
    path = tmp_path / "cities.csv"
    path.write_text("city\n東京\nZürich\nOslo\n\n", encoding="utf-8")
    current = dp.profile(str(path))
    delta = baseline.compare(current)

    assert delta["metric_semantics"]["comparable"] is None
    # 0.11 did not record which quality components were sampled.
    assert delta["quality_score"]["evidence"]["a"] == {
        "coverage": "incomplete",
        "reason": "coverage_unrecorded",
    }
    assert delta["quality_score"]["evidence"]["b"] == {"coverage": "complete"}
    assert delta["reports"]["a"]["evidence"] == {"coverage": "complete"}


def test_a_truncated_side_says_so(tmp_path: Path):
    path = tmp_path / "rows.csv"
    path.write_text("id,amount\n" + "".join(f"{i},{i * 2}\n" for i in range(200)))
    exact = dp.profile(str(path))
    truncated = dp.profile(str(path), max_rows=50)
    delta = exact.compare(truncated)

    truncated_evidence = {"coverage": "incomplete", "reason": "truncated"}
    assert delta["reports"]["a"]["evidence"] == {"coverage": "complete"}
    assert delta["reports"]["b"]["evidence"] == truncated_evidence
    assert delta["reports"]["b"]["rows_processed"] == 50
    assert delta["quality_score"]["evidence"]["b"] == truncated_evidence
    for change in delta["dimensions"].values():
        assert change["evidence"]["b"] == truncated_evidence


def test_nothing_assessed_invents_no_number():
    empty = dp.profile({"id": [], "amount": []})
    for delta in (empty.compare(dp.profile(CLEAN)), empty.compare(empty)):
        assert delta["quality_score"]["abs"] is None
        assert delta["quality_score"]["rel_pct"] is None
        assert all(change["abs"] is None for change in delta["dimensions"].values())
        assert all(change["null_pct_delta"] is None for change in delta["columns"].values())


def test_a_summary_that_records_only_its_row_count_still_compares():
    """The oldest accepted flat summary records `rows_processed` and nothing
    else about its scan, so its coverage is unrecorded, never complete."""
    legacy = dp.ProfileReport.from_dict(
        {"source": "old.csv", "columns": [], "execution": {"rows_processed": 5}}
    )
    unrecorded = {"coverage": "incomplete", "reason": "coverage_unrecorded"}
    for delta in (legacy.compare(legacy), legacy.compare(dp.profile(CLEAN))):
        assert delta["reports"]["a"]["rows_processed"] == 5
        assert delta["reports"]["a"]["evidence"] == unrecorded
        assert delta["quality_score"]["abs"] is None


def test_a_repeated_column_name_reads_its_first_profile_as_check_does():
    report = dp.profile(CLEAN)
    document = json.loads(report.to_json())
    first, second = dict(document["column_profiles"][1]), dict(document["column_profiles"][1])
    first["null_count"] = 0
    second["null_count"] = second["total_count"]
    document["column_profiles"] = [document["column_profiles"][0], first, second]
    rebuilt = dp.ProfileReport.from_dict(document)

    delta = rebuilt.compare(rebuilt)
    assert delta["schema"]["common"] == ["id", "amount"]
    assert delta["columns"]["amount"]["null_pct_a"] == 0.0
    observed = rebuilt.check(max_null_percentage={"amount": 50}).checks
    assert [c.observed for c in observed if c.column == "amount"] == [0.0]


def test_configuration_tells_a_setting_change_from_a_data_change():
    # The audit's example: unchanged data, one more hint, a lower score.
    data = {"v": [-3, -1, 2, 4]}
    plain = dp.profile(data)
    hinted = dp.profile(data, positive_columns=["v"])
    delta = plain.compare(hinted)

    assert delta["quality_score"]["abs"] < 0
    assert delta["metric_semantics"]["comparable"] is True
    hints = delta["configuration"]["semantic_hints"]
    assert hints["same"] is False
    assert hints["a"]["positive_columns"] == []
    assert hints["b"]["positive_columns"] == ["v"]
    assert delta["configuration"]["score_weights"]["same"] is True


def test_a_repeated_temporal_column_is_the_same_effective_configuration():
    # A repetition counts once, including in binding/configuration evidence (#1004).
    data = {
        "old": ["2001-01-01", "2002-01-01", "2003-01-01", "2026-10-01"],
        "new": ["2026-10-01", "2026-10-02", "2026-10-03", "2099-01-01"],
    }
    once = dp.profile(data, temporal_columns=["old", "new"])
    twice = dp.profile(data, temporal_columns=["old", "old", "new"])
    hints = once.compare(twice)["configuration"]["semantic_hints"]

    assert hints["same"] is True
    assert hints["b"]["temporal_columns"] == ["new", "old"]
    assert once.to_dict()["quality"] == twice.to_dict()["quality"]


def test_a_0_11_baseline_leaves_its_weights_unrecorded():
    text = (FIXTURES / "legacy_0_11_non_ascii.summary.json").read_text(encoding="utf-8")
    baseline = dp.ProfileReport.from_dict(json.loads(text))
    weights = baseline.compare(dp.profile(CLEAN))["configuration"]["score_weights"]

    # 0.11 summaries could drop custom weights, so absent is not the defaults.
    assert weights["a"] is None
    assert weights["same"] is None


def test_a_summary_from_before_0_10_leaves_its_hints_unrecorded():
    summary = dp.profile(CLEAN).to_dict()
    del summary["schema_version"]
    legacy = dp.ProfileReport.from_dict(summary)

    hints = legacy.compare(dp.profile(CLEAN))["configuration"]["semantic_hints"]

    assert hints["a"] is None
    assert hints["same"] is None
