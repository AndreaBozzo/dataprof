"""The baseline gate's null count increase rule, ``max_null_count_increase`` (#1346).

`tests/null_count_increase_gate.rs` is the Rust twin. Here the extra routes are
the Python ones: dicts, pandas and Arrow inputs, CSV bytes, and baselines read
back from ``to_json()`` and from a flat ``to_dict()`` summary, and the
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


def _column(rows: int, nulls: int) -> list[str | None]:
    """``rows`` values of which the first ``nulls`` are null."""
    return [None if i < nulls else f"v{i}" for i in range(rows)]


def _csv(values: list[str | None]) -> bytes:
    # A row number beside `k`, so a row whose `k` is empty is not a blank line.
    return ("k,i\n" + "".join(f"{value or ''},{i}\n" for i, value in enumerate(values))).encode()


def _frame(values: list[str | None]) -> dict[str, list[Any]]:
    return {"k": values, "i": list(range(len(values)))}


ROUTES: dict[str, Callable[[list[str | None]], dp.ProfileReport]] = {
    "dict": lambda values: dp.profile(_frame(values)),
    "pandas": lambda values: dp.profile(pd.DataFrame(_frame(values))),
    "arrow": lambda values: dp.profile(
        pa.table({"k": pa.array(values, pa.string()), "i": list(range(len(values)))})
    ),
    "csv bytes": lambda values: dp.profile(_csv(values), format="csv"),
}

BASELINE_FORMS: dict[str, Callable[[dp.ProfileReport], dp.ProfileReport]] = {
    "live": lambda report: report,
    "to_json": lambda report: dp.ProfileReport.from_json(report.to_json()),
    "to_dict": lambda report: dp.ProfileReport.from_dict(report.to_dict()),
}

#: Each case against a baseline of 2 nulls in 100 rows, allowing one more.
CASES: list[tuple[str, list[str | None], str]] = [
    ("unchanged", _column(100, 2), "passed"),
    ("one more null, within the allowance", _column(100, 3), "passed"),
    ("two more nulls", _column(100, 4), "failed"),
    ("fewer nulls", _column(100, 0), "passed"),
    ("more rows, same nulls", _column(200, 2), "passed"),
    ("every value null", _column(100, 100), "failed"),
]


def _status(result: dp.QualityGateResult) -> str:
    (check,) = [check for check in result.checks if check.code == "max_null_count_increase"]
    return check.status if check.reason is None else check.reason["reason"]


def test_a_rise_past_the_allowance_fails_on_every_route():
    wrong = []
    for route, build in ROUTES.items():
        baseline = build(_column(100, 2))
        for form, read in BASELINE_FORMS.items():
            for case, values, expected in CASES:
                result = build(values).check(
                    baseline=read(baseline), max_null_count_increase={"k": 1}
                )
                got = _status(result)
                if got != expected:
                    wrong.append(f"{route}/{form}/{case}: {got}, expected {expected}")
    assert not wrong, "\n".join(wrong)


def test_one_new_null_fails_where_the_rounded_percentage_passes():
    """The issue's example: the percentage rule passes from about 30,000 rows."""
    wrong = []
    for n in (20_000, 30_000):
        for route, build in ROUTES.items():
            base = build(_column(n, 0))
            cur = build(_column(n, 1))
            count = cur.check(baseline=base, max_null_count_increase={"k": 0}).verdict
            if count != "fail":
                wrong.append(f"{route}/{n}: count rule {count}")
            percentage = cur.check(baseline=base, max_null_percentage_increase={"k": 0}).verdict
            if percentage != ("fail" if n == 20_000 else "pass"):
                wrong.append(f"{route}/{n}: percentage rule {percentage}")
    assert not wrong, "\n".join(wrong)


def test_the_check_carries_both_counts_and_a_signed_change():
    baseline = dp.profile(_frame(_column(100, 5)))
    report = dp.profile(_frame(_column(100, 8)))
    result = report.check(baseline=baseline, max_null_count_increase={"k": 2})
    assert result.verdict == "fail"
    check = result.checks[0].to_dict()
    assert check["code"] == "max_null_count_increase"
    assert check["expected"] == {"comparison": "change_at_most", "value": 2}
    assert check["observed"] == 8
    assert check["baseline"]["observed"] == 5
    assert check["baseline"]["change"] == 3
    fell = baseline.check(baseline=report, max_null_count_increase=0).checks[0].to_dict()
    assert fell["baseline"]["change"] == -3
    assert fell["status"] == "passed"
    # Whole numbers, never widened into floats.
    assert all(isinstance(value, int) for value in (check["observed"], fell["baseline"]["change"]))


def test_a_summary_without_the_counts_is_not_in_baseline():
    baseline = dp.profile(_frame(_column(100, 2)))
    report = dp.profile(_frame(_column(100, 9)))
    wrong = []
    for field in ("null_count", "total_count"):
        summary: dict[str, Any] = json.loads(json.dumps(baseline.to_dict()))
        (column,) = [c for c in summary["columns"] if c["name"] == "k"]
        column.pop(field)
        result = report.check(
            baseline=dp.ProfileReport.from_dict(summary), max_null_count_increase={"k": 0}
        )
        if _status(result) != "not_in_baseline":
            wrong.append(f"summary without {field}: {_status(result)}")
    assert not wrong, "\n".join(wrong)


def test_the_wildcard_and_its_validation():
    baseline = dp.profile({"a": [1, 2, 3, 4], "b": [1, 2, 3, 4]})
    report = dp.profile({"a": [None, None, 3, 4], "b": [1, 2, 3, 4]})
    for policy in (0, {"*": 0}):
        result = report.check(baseline=baseline, max_null_count_increase=policy)
        assert [(check.column, check.status) for check in result.checks] == [
            ("a", "failed"),
            ("b", "passed"),
        ]
    named = report.check(baseline=baseline, max_null_count_increase={"*": 0, "a": 2})
    assert [(check.column, check.status) for check in named.checks] == [
        ("a", "passed"),
        ("b", "passed"),
    ]
    with pytest.raises(ValueError, match="baseline"):
        report.check(max_null_count_increase={"a": 0})
    bad_limits: tuple[Any, ...] = (-1, 1.5, True, "1")
    for bad in bad_limits:
        with pytest.raises(ValueError, match="max_null_count_increase"):
            report.check(baseline=baseline, max_null_count_increase={"a": bad})
        with pytest.raises(ValueError, match="max_null_count_increase"):
            report.check(baseline=baseline, max_null_count_increase=bad)


def test_the_check_entrypoint_flag_and_policy_key(tmp_path: Path):
    baseline = tmp_path / "baseline.json"
    dp.profile(_csv(_column(100, 2)), format="csv").save(baseline)
    source = tmp_path / "today.csv"
    source.write_bytes(_csv(_column(100, 5)))
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({"max_null_count_increase": {"k": 2}}), encoding="utf-8")

    against = [str(source), "--baseline", str(baseline)]
    assert entrypoint.main([*against, "--max-null-count-increase", "k=2"]) == 1
    assert entrypoint.main([*against, "--max-null-count-increase", "*=2"]) == 1
    assert entrypoint.main([*against, "--policy", str(policy)]) == 1
    assert entrypoint.main([*against, "--max-null-count-increase", "k=3"]) == 0
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"max_null_count_increase": {"k": -1}}), encoding="utf-8")
    assert entrypoint.main([*against, "--policy", str(bad)]) == 3
