"""The baseline gate's distinct-count collapse rule, ``max_distinct_drop`` (#1026).

`tests/distinct_drop_gate.rs` is the Rust twin. Here the extra routes are the
Python ones: dicts, pandas and Arrow inputs, CSV bytes, and baselines read back
from ``to_json()`` and from a flat ``to_dict()`` summary, and the
``python -m dataprof.check`` flag and policy-file key.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import dataprof as dp
import pytest
from dataprof.check import __main__ as entrypoint

pa = pytest.importorskip("pyarrow")
pd = pytest.importorskip("pandas")


def _keys(rows: int, distinct: int) -> list[str | None]:
    return [f"c{i % distinct}" for i in range(rows)]


def _csv(values: list[str | None]) -> bytes:
    return ("k\n" + "".join(f"{value or ''}\n" for value in values)).encode()


ROUTES: dict[str, Callable[[list[str | None]], dp.ProfileReport]] = {
    "dict": lambda values: dp.profile({"k": values}),
    "pandas": lambda values: dp.profile(pd.DataFrame({"k": values})),
    "arrow": lambda values: dp.profile(pa.table({"k": pa.array(values, pa.string())})),
    "csv bytes": lambda values: dp.profile(_csv(values), format="csv"),
}

BASELINE_FORMS: dict[str, Callable[[dp.ProfileReport], dp.ProfileReport]] = {
    "live": lambda report: report,
    "to_json": lambda report: dp.ProfileReport.from_json(report.to_json()),
    "to_dict": lambda report: dp.ProfileReport.from_dict(report.to_dict()),
}

#: Each case against a baseline of 100 distinct keys over 100 rows.
CASES: list[tuple[str, list[str | None], str]] = [
    ("unchanged", _keys(100, 100), "passed"),
    ("a key that repeats", _keys(100, 10), "failed"),
    ("fewer rows, all distinct", _keys(10, 10), "passed"),
    ("within the allowance", _keys(100, 90), "passed"),
    ("just past the allowance", _keys(100, 89), "failed"),
    ("every value null", [None] * 100, "not_assessed"),
]


def _status(result: dp.QualityGateResult) -> str:
    (check,) = [check for check in result.checks if check.code == "max_distinct_drop"]
    return check.status if check.reason is None else check.reason["reason"]


def test_a_collapse_fails_and_fewer_rows_or_held_counts_pass_on_every_route():
    wrong = []
    for route, build in ROUTES.items():
        baseline = build(_keys(100, 100))
        for form, read in BASELINE_FORMS.items():
            for case, values, expected in CASES:
                result = build(values).check(baseline=read(baseline), max_distinct_drop={"k": 10})
                got = _status(result)
                if got != expected:
                    wrong.append(f"{route}/{form}/{case}: {got}, expected {expected}")
    assert not wrong, "\n".join(wrong)


def test_the_check_carries_both_counts_and_the_relative_change():
    baseline = dp.profile({"k": _keys(100, 100)})
    result = dp.profile({"k": _keys(100, 10)}).check(baseline=baseline, max_distinct_drop={"k": 10})
    assert result.verdict == "fail"
    check = result.checks[0].to_dict()
    assert check["code"] == "max_distinct_drop"
    assert check["expected"] == {"comparison": "relative_change_at_least", "value": -10.0}
    assert check["observed"] == 10
    assert check["baseline"]["observed"] == 100
    assert check["baseline"]["change"] == -90.0


def test_an_estimated_or_unrecorded_count_is_never_decided():
    baseline = dp.profile({"k": _keys(100, 100)})
    report = dp.profile({"k": _keys(100, 10)})
    wrong = []

    def status_against(base: dp.ProfileReport) -> str:
        return _status(report.check(baseline=base, max_distinct_drop={"k": 10}))

    document = json.loads(baseline.to_json())
    document["column_profiles"][0]["unique_count_is_approximate"] = True
    got = status_against(dp.ProfileReport.from_json(json.dumps(document)))
    if got != "approximate_count":
        wrong.append(f"approximate baseline document: {got}")

    summary = baseline.to_dict()
    for label, edit in [
        ("approximate", lambda column: column.update(unique_count_is_approximate=True)),
        ("null", lambda column: column.update(unique_count_is_approximate=None)),
        ("absent", lambda column: column.pop("unique_count_is_approximate")),
    ]:
        edited: dict[str, Any] = json.loads(json.dumps(summary))
        edit(edited["columns"][0])
        got = status_against(dp.ProfileReport.from_dict(edited))
        if got != "approximate_count":
            wrong.append(f"summary flag {label}: {got}")
    assert not wrong, "\n".join(wrong)


def test_the_wildcard_and_its_validation():
    baseline = dp.profile({"a": [1, 2, 3, 4], "b": [1, 2, 3, 4]})
    report = dp.profile({"a": [1, 1, 1, 1], "b": [1, 2, 3, 4]})
    for policy in (10, {"*": 10}):
        result = report.check(baseline=baseline, max_distinct_drop=policy)
        assert [(check.column, check.status) for check in result.checks] == [
            ("a", "failed"),
            ("b", "passed"),
        ]
    named = report.check(baseline=baseline, max_distinct_drop={"*": 10, "a": 100})
    assert [(check.column, check.status) for check in named.checks] == [
        ("a", "passed"),
        ("b", "passed"),
    ]
    with pytest.raises(ValueError, match="baseline"):
        report.check(max_distinct_drop={"a": 10})
    with pytest.raises(ValueError, match="max_distinct_drop"):
        report.check(baseline=baseline, max_distinct_drop={"a": 150})


def test_the_check_entrypoint_flag_and_policy_key(tmp_path: Path):
    baseline = tmp_path / "baseline.json"
    dp.profile(_csv(_keys(100, 100)), format="csv").save(baseline)
    source = tmp_path / "today.csv"
    source.write_bytes(_csv(_keys(100, 10)))
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({"max_distinct_drop": {"k": 10}}), encoding="utf-8")

    flag = [str(source), "--baseline", str(baseline), "--max-distinct-drop", "k=10"]
    assert entrypoint.main(flag) == 1
    assert entrypoint.main([str(source), "--baseline", str(baseline), "--policy", str(policy)]) == 1
    held = [str(source), "--baseline", str(baseline), "--max-distinct-drop", "k=95"]
    assert entrypoint.main(held) == 0


def test_a_column_without_a_distinct_count_is_not_assessed():
    # A nested column holds values but is only counted.
    baseline = dp.profile({"n": [{"x": i} for i in range(4)]})
    report = dp.profile({"n": [{"x": 1}] * 4})
    result = report.check(baseline=baseline, max_distinct_drop={"n": 10})
    assert _status(result) == "not_assessed"
    assert result.checks[0].message == "the report counted no distinct values for this column"
