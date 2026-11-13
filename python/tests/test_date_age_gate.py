"""The gate's freshness ceiling, ``max_date_age`` (#1290).

`tests/date_age_gate.rs` is the Rust twin. Here the extra routes are the
Python ones: dicts of strings and of dates, pandas and Arrow inputs, CSV
bytes, a report read back from ``to_json()``, and the ``python -m
dataprof.check`` flag and policy-file key.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import dataprof as dp
import pytest
from dataprof.check import __main__ as entrypoint

pa = pytest.importorskip("pyarrow")
pd = pytest.importorskip("pandas")

DATES = ["2025-12-15", "2025-12-16", "2025-12-17"]


def _csv(dates: list[str]) -> bytes:
    return ("published_at,x\n" + "".join(f"{d},{i}\n" for i, d in enumerate(dates))).encode()


def _days(dates: list[str]) -> list[dt.date]:
    return [dt.date.fromisoformat(d) for d in dates]


ROUTES: dict[str, Callable[[list[str], str], dp.ProfileReport]] = {
    "dict of strings": lambda dates, ref: dp.profile(
        {"published_at": dates, "x": list(range(len(dates)))}, reference_date=ref
    ),
    "dict of dates": lambda dates, ref: dp.profile(
        {"published_at": _days(dates), "x": list(range(len(dates)))}, reference_date=ref
    ),
    "pandas datetime64": lambda dates, ref: dp.profile(
        pd.DataFrame({"published_at": pd.to_datetime(dates), "x": range(len(dates))}),
        reference_date=ref,
    ),
    "arrow date32": lambda dates, ref: dp.profile(
        pa.table({"published_at": pa.array(_days(dates)), "x": list(range(len(dates)))}),
        reference_date=ref,
    ),
    "arrow timestamp utc": lambda dates, ref: dp.profile(
        pa.table(
            {
                "published_at": pa.array(
                    [dt.datetime.fromisoformat(d + "T23:30:00") for d in dates],
                    pa.timestamp("us", tz="UTC"),
                ),
                "x": list(range(len(dates))),
            }
        ),
        reference_date=ref,
    ),
    "csv bytes": lambda dates, ref: dp.profile(_csv(dates), format="csv", reference_date=ref),
    "to_json": lambda dates, ref: dp.ProfileReport.from_json(
        dp.profile(_csv(dates), format="csv", reference_date=ref).to_json()
    ),
}


def _check(result: dp.QualityGateResult) -> dp.QualityCheck:
    (check,) = [check for check in result.checks if check.code == "max_date_age"]
    return check


def _status(check: dp.QualityCheck) -> str:
    return check.status if check.reason is None else check.reason["reason"]


#: (case, reference date, allowance in days, status, age in days)
CASES: list[tuple[str, str, int, str, int]] = [
    ("287 days stale", "2026-09-30", 3, "failed", 287),
    ("one day past the allowance", "2025-12-21", 3, "failed", 4),
    ("at the allowance", "2025-12-20", 3, "passed", 3),
    ("same day, zero allowance", "2025-12-17", 0, "passed", 0),
    ("newest date in the future", "2025-12-10", 0, "passed", -7),
]


def test_every_python_route_counts_the_same_age():
    wrong = []
    for case, reference, days, expected, age in CASES:
        for route, profile in ROUTES.items():
            check = _check(profile(DATES, reference).check(max_date_age={"published_at": days}))
            if (_status(check), check.observed) != (expected, age):
                wrong.append(f"{case} [{route}]: {_status(check)} {check.observed}")
    assert not wrong, "\n".join(wrong)


def test_the_issue_case_fails_where_every_other_rule_passes():
    report = dp.profile(_csv(DATES), format="csv", reference_date="2026-09-30")
    policy: dict[str, Any] = {
        "min_rows": 1,
        "require_columns": ["published_at"],
        "max_null_count": {"published_at": 0},
    }
    assert report.check(**policy).verdict == "pass"
    result = report.check(**policy, max_date_age={"published_at": 3})
    assert result.verdict == "fail"
    assert _check(result).to_dict() == {
        "code": "max_date_age",
        "column": "published_at",
        "expected": {"comparison": "at_most", "value": 3},
        "observed": 287,
        "scope": "full_source",
        "evidence": {"coverage": "complete"},
        "status": "failed",
        "message": "this column's newest date is older than the allowed age",
    }


def test_a_sampled_newest_date_decides_only_what_unread_values_cannot_reverse(
    tmp_path: Path,
):
    start = dt.date(2020, 1, 1)
    stale = [str(start + dt.timedelta(days=i % 1000)) for i in range(30_000)]
    path = tmp_path / "stale.csv"
    path.write_bytes(_csv(stale))
    report = dp.profile(str(path), reference_date="2026-09-30")
    full = _check(report.check(max_date_age={"published_at": 3}))
    assert (_status(full), full.evidence) == (
        "evidence_incomplete",
        {"coverage": "incomplete", "reason": "quality_sampled"},
    )
    observed = _check(report.check(max_date_age={"published_at": 3}, scope="observed"))
    assert observed.status == "failed"

    path.write_bytes(_csv(["2026-09-29"] * 30_000))
    fresh = _check(
        dp.profile(str(path), reference_date="2026-09-30").check(max_date_age={"published_at": 3})
    )
    assert (fresh.status, fresh.observed) == ("passed", 1)

    # CSV bytes are profiled in memory, over every value.
    in_memory = _check(
        dp.profile(_csv(stale), format="csv", reference_date="2026-09-30").check(
            max_date_age={"published_at": 3}
        )
    )
    assert (in_memory.status, in_memory.evidence) == ("failed", {"coverage": "complete"})


def test_a_column_without_a_newest_date_is_never_a_pass():
    report = dp.profile(_csv(DATES), format="csv", reference_date="2026-09-30")
    cases = {
        "x": "not_assessed",
        "missing": "column_not_profiled",
    }
    for column, reason in cases.items():
        result = report.check(max_date_age={column: 3})
        assert (result.verdict, _status(_check(result))) == ("inconclusive", reason), column
    all_null = dp.profile(
        pa.table({"published_at": pa.array([None, None], pa.date32())}),
        reference_date="2026-09-30",
    )
    assert _status(_check(all_null.check(max_date_age={"published_at": 3}))) == "not_assessed"
    no_quality = dp.profile(
        _csv(DATES), format="csv", reference_date="2026-09-30", metrics=["schema", "statistics"]
    )
    assert (
        _status(_check(no_quality.check(max_date_age={"published_at": 3}))) == "quality_unavailable"
    )
    document = json.loads(report.to_json())
    del document["quality"]["metrics"]["timeliness"]["clock"]
    no_clock = _check(
        dp.ProfileReport.from_json(json.dumps(document)).check(max_date_age={"published_at": 3})
    )
    assert no_clock.reason == {"reason": "setting_unrecorded", "setting": "timeliness_clock"}


def test_the_allowance_must_be_whole_days_in_a_mapping():
    report = dp.profile(_csv(DATES), format="csv", reference_date="2026-09-30")
    for bad in (-1, 1.5, "3", True, 1 << 32):
        with pytest.raises(ValueError, match="max_date_age for `published_at` must be"):
            report.check(max_date_age={"published_at": bad})  # ty: ignore[invalid-argument-type]
    with pytest.raises(ValueError, match="max_date_age must map column names"):
        report.check(max_date_age=3)  # ty: ignore[invalid-argument-type]
    with pytest.raises(ValueError, match="no '\\*' form"):
        report.check(max_date_age={"*": 3})
    assert report.check(max_date_age={"published_at": (1 << 32) - 1}).verdict == "pass"


def test_the_check_entrypoint_flag_and_policy_key(tmp_path: Path):
    # The command line reads today's date, so the dates sit far from it.
    source = tmp_path / "orders.csv"
    source.write_bytes(_csv(["2020-01-01"]))
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({"max_date_age": {"published_at": 3}}), encoding="utf-8")

    assert entrypoint.main([str(source), "--max-date-age", "published_at=3"]) == 1
    assert entrypoint.main([str(source), "--policy", str(policy)]) == 1
    assert entrypoint.main([str(source), "--max-date-age", "x=3"]) == 2

    source.write_bytes(_csv(["2999-01-01"]))
    assert entrypoint.main([str(source), "--max-date-age", "published_at=3"]) == 0
    assert entrypoint.main([str(source), "--policy", str(policy)]) == 0
