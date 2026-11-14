"""The largest gap between a date column's days, and ``max_date_gap`` (#1300).

`tests/date_gap_gate.rs` is the Rust twin; it covers every file engine, the
async route and capped scans. Here the extra routes are the Python ones: dicts
of strings and of dates, pandas and Arrow inputs, CSV bytes, a report read back
from ``to_json()``, and the ``python -m dataprof.check`` flag and policy-file
key.
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


def _daily(skip: Callable[[dt.date], bool] = lambda _: False) -> list[str]:
    start = dt.date(2024, 1, 1)
    days = (start + dt.timedelta(days=offset) for offset in range(182))
    return [str(day) for day in days if not skip(day)]


CONTIGUOUS = _daily()
#: March removed: 29 February to 1 April is 32 days.
HOLED = _daily(lambda day: day.month == 3)


def _csv(dates: list[str]) -> bytes:
    return ("published_at,x\n" + "".join(f"{d},{i}\n" for i, d in enumerate(dates))).encode()


def _days(dates: list[str]) -> list[dt.date]:
    return [dt.date.fromisoformat(d) for d in dates]


def _x(dates: list[str]) -> list[int]:
    return list(range(len(dates)))


ROUTES: dict[str, Callable[[list[str]], dp.ProfileReport]] = {
    "dict of strings": lambda dates: dp.profile({"published_at": dates, "x": _x(dates)}),
    "dict of dates": lambda dates: dp.profile({"published_at": _days(dates), "x": _x(dates)}),
    "pandas datetime64": lambda dates: dp.profile(
        pd.DataFrame({"published_at": pd.to_datetime(dates), "x": _x(dates)})
    ),
    "arrow date32": lambda dates: dp.profile(
        pa.table({"published_at": pa.array(_days(dates)), "x": _x(dates)})
    ),
    "arrow timestamp utc": lambda dates: dp.profile(
        pa.table(
            {
                "published_at": pa.array(
                    [dt.datetime.fromisoformat(d + "T23:30:00") for d in dates],
                    pa.timestamp("us", tz="UTC"),
                ),
                "x": _x(dates),
            }
        )
    ),
    "csv bytes": lambda dates: dp.profile(_csv(dates), format="csv"),
    "to_json": lambda dates: dp.ProfileReport.from_json(
        dp.profile(_csv(dates), format="csv").to_json()
    ),
}


def _check(result: dp.QualityGateResult) -> dp.QualityCheck:
    (check,) = [check for check in result.checks if check.code == "max_date_gap"]
    return check


def _status(check: dp.QualityCheck) -> str:
    return check.status if check.reason is None else check.reason["reason"]


def _largest_gap(report: dp.ProfileReport) -> Any:
    document = json.loads(report.to_json())
    (column,) = [c for c in document["column_profiles"] if c["name"] == "published_at"]
    return column["stats"]["DateTime"].get("largest_gap")


#: (case, dates, allowance in days, status, recorded gap)
CASES: list[tuple[str, list[str], int, str, dict[str, Any]]] = [
    ("contiguous", CONTIGUOUS, 1, "passed", {"days": 1, "from": "2024-01-01", "to": "2024-01-02"}),
    ("March removed", HOLED, 1, "failed", {"days": 32, "from": "2024-02-29", "to": "2024-04-01"}),
    (
        "March removed, at the allowance",
        HOLED,
        32,
        "passed",
        {"days": 32, "from": "2024-02-29", "to": "2024-04-01"},
    ),
    (
        "one day",
        ["2024-05-06"] * 3,
        0,
        "passed",
        {"days": 0, "from": "2024-05-06", "to": "2024-05-06"},
    ),
]


def test_every_python_route_reports_the_same_gap_and_decision():
    wrong = []
    for case, dates, days, expected, gap in CASES:
        for label, route in ROUTES.items():
            report = route(dates)
            if _largest_gap(report) != gap:
                wrong.append(f"{case} [{label}] gap: {_largest_gap(report)}")
            check = _check(report.check(max_date_gap={"published_at": days}))
            if (_status(check), check.observed) != (expected, gap["days"]):
                wrong.append(f"{case} [{label}]: {_status(check)} {check.observed}")
    assert not wrong, f"{len(wrong)} wrong:\n" + "\n".join(wrong)


def test_the_hole_fails_where_the_newest_date_and_rows_pass():
    report = dp.profile(_csv(HOLED), format="csv", reference_date="2024-06-30")
    policy: dict[str, Any] = {
        "min_rows": 100,
        "max_date_age": {"published_at": 0},
        "max_null_count": {"published_at": 0},
    }
    assert report.check(**policy).verdict == "pass"
    result = report.check(**policy, max_date_gap={"published_at": 1})
    assert result.verdict == "fail"
    assert _check(result).message == (
        "this column's dates hold a gap of 32 days from 2024-02-29 to 2024-04-01, "
        "longer than allowed"
    )


def test_a_capped_scan_is_inconclusive_either_way(tmp_path: Path):
    path = tmp_path / "holed.csv"
    path.write_bytes(_csv(HOLED))
    for rows, gap in ((59, 1), (70, 32)):
        report = dp.profile(str(path), max_rows=rows)
        full = _check(report.check(max_date_gap={"published_at": 1}))
        assert (full.reason, full.observed) == (
            {"reason": "evidence_incomplete", "gap": "truncated"},
            gap,
        ), rows
        observed = _check(report.check(max_date_gap={"published_at": 1}, scope="observed"))
        assert observed.status == ("passed" if gap == 1 else "failed"), rows


def test_a_column_without_a_gap_is_never_a_pass():
    report = dp.profile(_csv(HOLED), format="csv")
    for column, reason in {"x": "not_assessed", "missing": "column_not_profiled"}.items():
        result = report.check(max_date_gap={column: 1})
        assert (result.verdict, _status(_check(result))) == ("inconclusive", reason), column
    all_null = dp.profile(pa.table({"published_at": pa.array([None, None], pa.date32())}))
    assert _status(_check(all_null.check(max_date_gap={"published_at": 1}))) == "not_assessed"
    # A report written before the field.
    document = json.loads(report.to_json())
    for column in document["column_profiles"]:
        column["stats"].get("DateTime", {}).pop("largest_gap", None)
    old = dp.ProfileReport.from_json(json.dumps(document))
    assert _status(_check(old.check(max_date_gap={"published_at": 1}))) == "not_assessed"


def test_the_allowance_must_be_whole_days_in_a_mapping():
    report = dp.profile(_csv(HOLED), format="csv")
    for bad in (-1, 1.5, "3", True, 1 << 32):
        with pytest.raises(ValueError, match="max_date_gap for `published_at` must be"):
            report.check(max_date_gap={"published_at": bad})  # ty: ignore[invalid-argument-type]
    with pytest.raises(ValueError, match="max_date_gap must map column names"):
        report.check(max_date_gap=3)  # ty: ignore[invalid-argument-type]
    with pytest.raises(ValueError, match="no '\\*' form"):
        report.check(max_date_gap={"*": 3})
    assert report.check(max_date_gap={"published_at": (1 << 32) - 1}).verdict == "pass"


def test_the_check_entrypoint_flag_and_policy_key(tmp_path: Path):
    source = tmp_path / "daily.csv"
    source.write_bytes(_csv(HOLED))
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({"max_date_gap": {"published_at": 1}}), encoding="utf-8")

    assert entrypoint.main([str(source), "--max-date-gap", "published_at=1"]) == 1
    assert entrypoint.main([str(source), "--policy", str(policy)]) == 1
    assert entrypoint.main([str(source), "--max-date-gap", "published_at=32"]) == 0
    assert entrypoint.main([str(source), "--max-date-gap", "x=1"]) == 2

    source.write_bytes(_csv(CONTIGUOUS))
    assert entrypoint.main([str(source), "--max-date-gap", "published_at=1"]) == 0
    assert entrypoint.main([str(source), "--policy", str(policy)]) == 0
