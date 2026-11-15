"""Timeliness reads slash dates in the order the date statistics record (#1305).

`tests/timeliness_slash_order.rs` is the Rust twin. Here every route that
keeps a sample (file engines, pandas, Arrow) is held to the verdict of the
same values in a dict, which keeps every value, and so is a report read back
from ``to_json()``.

Each case holds 40,000 ambiguous slash dates and, in its last row, past the
sample, the one value that only parses month-first.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import dataprof as dp
import pytest

pa = pytest.importorskip("pyarrow")
pd = pytest.importorskip("pandas")

ROWS = 40_000
REFERENCE = "2024-06-01"

# label: {column: (ambiguous value, settling value)}
CASES: dict[str, dict[str, tuple[str, str]]] = {
    # Month-first 1 December 2024, after the reference date.
    "future month-first": {"event_date": ("12/01/2024", "12/31/2023")},
    # Month-first 12 January 2024; day-first it would be 1 December.
    "past month-first": {"event_date": ("01/12/2024", "12/31/2023")},
    # Month-first every pair runs forwards; day-first every pair is inverted.
    "pair month-first": {
        "start_date": ("01/05/2024", "12/30/2023"),
        "end_date": ("02/03/2024", "12/31/2023"),
    },
}


def _columns(case: dict[str, tuple[str, str]]) -> dict[str, list[str]]:
    return {
        name: [ambiguous] * (ROWS - 1) + [settling] for name, (ambiguous, settling) in case.items()
    }


def _csv(columns: dict[str, list[str]]) -> str:
    names = list(columns)
    lines = [",".join(names)]
    lines += [",".join(row) for row in zip(*(columns[name] for name in names), strict=True)]
    return "\n".join(lines) + "\n"


def _routes(tmp_path: Path) -> dict[str, Callable[[dict[str, list[str]]], dp.ProfileReport]]:
    def file(engine: str, **kwargs: Any) -> Callable[[dict[str, list[str]]], dp.ProfileReport]:
        def run(columns: dict[str, list[str]]) -> dp.ProfileReport:
            path = tmp_path / f"{engine}.csv"
            path.write_text(_csv(columns))
            return dp.profile(str(path), engine=engine, reference_date=REFERENCE, **kwargs)

        return run

    return {
        "csv auto": file("auto"),
        "csv incremental": file("incremental"),
        "csv columnar": file("columnar"),
        # Without the statistics pack the quality block must not change (#1268).
        "csv auto quality only": file("auto", metrics=["quality"]),
        "arrow quality only": lambda columns: dp.profile(
            pa.table(columns), reference_date=REFERENCE, metrics=["quality"]
        ),
        "csv bytes": lambda columns: dp.profile(
            _csv(columns).encode(), format="csv", reference_date=REFERENCE
        ),
        "pandas": lambda columns: dp.profile(pd.DataFrame(columns), reference_date=REFERENCE),
        "arrow": lambda columns: dp.profile(pa.table(columns), reference_date=REFERENCE),
    }


def _verdicts(report: dp.ProfileReport) -> tuple[str, str]:
    return (
        report.check(min_dimension_scores={"timeliness": 90}).verdict,
        report.check(forbid_findings=["future_dates"]).verdict,
    )


def test_every_route_gives_the_verdict_of_the_dict(tmp_path: Path) -> None:
    wrong = []
    for label, case in CASES.items():
        columns = _columns(case)
        oracle = dp.profile(columns, reference_date=REFERENCE)
        expected = _verdicts(oracle)
        assert expected == (
            ("fail", "fail") if label == "future month-first" else ("pass", "pass")
        ), label
        stats = json.loads(oracle.to_json())["column_profiles"][0]["stats"]["DateTime"]
        assert stats["slash_date_order"] == "month_first", label

        for route, run in _routes(tmp_path).items():
            report = run(columns)
            saved = dp.ProfileReport.from_json(report.to_json())
            for form, got in (("run", _verdicts(report)), ("saved", _verdicts(saved))):
                score_ok = got[0] == expected[0]
                # A sample with no future date cannot show the rest holds none.
                findings_ok = (
                    got[1] in {expected[1], "inconclusive"}
                    if expected[1] == "pass"
                    else got[1] == "fail"
                )
                if not (score_ok and findings_ok):
                    wrong.append(f"{label} [{route}, {form}]: {got}, dict {expected}")
    assert not wrong, "\n".join(wrong)


def test_every_route_compares_the_pair_in_the_recorded_order(tmp_path: Path) -> None:
    columns = _columns(CASES["pair month-first"])
    wrong = []
    for route, run in _routes(tmp_path).items():
        timeliness = json.loads(run(columns).to_json())["quality"]["metrics"]["timeliness"]
        if timeliness["temporal_pairs_checked"] == 0 or timeliness["temporal_violations"] != 0:
            wrong.append(f"{route}: {timeliness}")
    assert not wrong, "\n".join(wrong)


def test_a_quality_only_run_reports_no_date_statistics(tmp_path: Path) -> None:
    columns = _columns(CASES["past month-first"])
    for route, run in _routes(tmp_path).items():
        if not route.endswith("quality only"):
            continue
        document = json.loads(run(columns).to_json())
        assert [column["stats"] for column in document["column_profiles"]] == ["None"], route
