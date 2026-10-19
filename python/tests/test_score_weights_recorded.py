"""A report names the weights behind its overall score, defaults included (#749).

``tests/score_weights_recorded.rs`` is the Rust twin. This side also covers a
flat summary written before 0.12, which dropped custom weights: re-exporting
it must keep the weights absent rather than claim the defaults.
"""

from __future__ import annotations

import json

import dataprof as dp

DATA = {"id": [1, 2, 3, 4], "amount": [10, 20, 30, 40]}
DEFAULTS = {
    "completeness": 0.25,
    "consistency": 0.2,
    "uniqueness": 0.15,
    "accuracy": 0.15,
    "timeliness": 0.1,
    "validity": 0.1,
    "precision": 0.05,
}


def test_default_weights_are_written_in_every_document():
    report = dp.profile(DATA)
    canonical = json.loads(report.to_json())
    summary = report.to_dict()
    rewritten = json.loads(dp.ProfileReport.from_dict(canonical).to_json())

    documents = {
        "canonical": canonical["quality"]["metrics"].get("score_weights"),
        "summary": summary["quality"].get("score_weights"),
        "canonical after a round trip": rewritten["quality"]["metrics"].get("score_weights"),
    }
    wrong = {name: weights for name, weights in documents.items() if weights != DEFAULTS}
    assert not wrong, wrong


def test_a_flat_summary_without_weights_is_re_exported_without_them():
    summary = dp.profile(DATA).to_dict()
    del summary["quality"]["score_weights"]

    exported = dp.ProfileReport.from_dict(summary).to_dict()

    assert "score_weights" not in exported["quality"]


def test_a_flat_summary_keeps_the_weights_it_recorded():
    summary = dp.profile(DATA).to_dict()

    exported = dp.ProfileReport.from_dict(summary).to_dict()

    assert exported["quality"]["score_weights"] == DEFAULTS
