"""ProfileReport.compare() returns the Rust report delta (#713).

`tests/report_delta.rs` is the Rust twin. Here the extra routes are the
Python ones: a report loaded from a flat `to_dict()` summary, including a
summary written by 0.11, compares through the same delta as a profiled one.
"""

import json
from pathlib import Path

import dataprof as dp

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
    }
    assert set(delta["metric_semantics"]) == {"a", "b", "comparable"}
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
