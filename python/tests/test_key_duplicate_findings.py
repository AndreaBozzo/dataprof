"""Declared-key findings require exact counts over a fully read source."""

from __future__ import annotations

import json
from typing import Any

import dataprof as dp
import pytest

_ABSENT = object()


@pytest.fixture
def keyed_report(tmp_path):
    path = tmp_path / "orders.csv"
    path.write_text("order_id,amount\nA,10\nB,20\nA,30\n,40\n,50\n", encoding="utf-8")
    return dp.profile_file(path, identifier_columns=["order_id"])


def test_repeated_declared_keys_use_counts_not_the_rounded_share(keyed_report):
    document = keyed_report.to_dict()
    document["quality"]["uniqueness"]["key_uniqueness"] = 100.0
    report = dp.ProfileReport.from_dict(document)
    finding = next(f for f in report.findings() if f.code == "key_duplicates")
    assert finding.to_dict() == {
        "code": "key_duplicates",
        "severity": "warning",
        "column": "order_id",
        "evidence": {
            "key_column": "order_id",
            "key_values_checked": 3,
            "repeated_values": 1,
            "key_column_source": "declared",
        },
        "summary": "the declared key contains repeated non-null values",
    }
    assert not any(f.code == "duplicate_rows" for f in report.findings())
    restored = dp.ProfileReport.from_json(report.to_json())
    assert restored.findings().to_dict() == report.findings().to_dict()
    assert json.loads(report.findings().to_json()) == report.findings().to_dict()


@pytest.mark.parametrize(
    ("path", "value", "reason"),
    [
        (("columns", 0, "unique_count_is_approximate"), True, "estimated"),
        (("columns", 0, "unique_count_is_approximate"), None, "unrecorded"),
        (("columns", 0, "unique_count"), None, "not_computed"),
        (("columns", 0, "unique_count"), 4, "unrecorded"),
        (("execution", "source_exhausted"), False, "partial_scan"),
        (("execution", "sampling_applied"), True, "partial_scan"),
        (("metric_semantics",), _ABSENT, "unrecorded"),
        (("metric_semantics", "key_column_source"), _ABSENT, "unrecorded"),
        (("quality", "sampled_dimensions"), ["key_uniqueness"], "sampled"),
        (("quality", "sampled_dimensions"), None, "unrecorded"),
        (("quality", "uniqueness", "key_values_checked"), None, "unrecorded"),
        (("quality", "uniqueness", "key_values_checked"), 4, "unrecorded"),
    ],
)
def test_incomplete_key_evidence_is_unevaluated(keyed_report, path, value, reason):
    document = keyed_report.to_dict()
    parent: Any = document
    for key in path[:-1]:
        parent = parent[key]
    if value is _ABSENT:
        del parent[path[-1]]
    else:
        parent[path[-1]] = value
    result = dp.ProfileReport.from_dict(document).findings()
    assert not any(f.code == "key_duplicates" for f in result)
    assert {"code": "key_duplicates", "reason": reason, "columns": ["order_id"]} in (
        result.not_evaluated
    )


def test_null_keys_are_not_repeated_keys(tmp_path):
    path = tmp_path / "empty-keys.csv"
    path.write_text("order_id,amount\n,10\n,20\n", encoding="utf-8")
    result = dp.profile_file(path, identifier_columns=["order_id"]).findings()
    assert not any(f.code == "key_duplicates" for f in result)
    assert {
        "code": "key_duplicates",
        "reason": "no_values",
        "columns": ["order_id"],
    } in result.not_evaluated


@pytest.mark.parametrize("field", ["total_count", "null_count"])
def test_key_rule_does_not_guess_missing_legacy_counts(keyed_report, field):
    # Flat legacy reports can omit these counts. Exercise this rule alone:
    # the pre-existing column rules require counts before they can evaluate.
    from importlib import import_module

    findings: Any = import_module("dataprof._findings")

    document = keyed_report.to_dict()
    del document["columns"][0][field]
    report = dp.ProfileReport.from_dict(document)
    assert report.quality is not None
    out = findings._Collector()
    findings._key_duplicate_findings(report, out, report.quality.uniqueness)
    assert out.finish().to_dict() == {
        "findings": [],
        "not_evaluated": [
            {"code": "key_duplicates", "reason": "unrecorded", "columns": ["order_id"]}
        ],
    }
