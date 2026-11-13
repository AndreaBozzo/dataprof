"""The baseline gate's score drop rules on sampled scores (#1035).

`tests/score_drop_intervals.rs` is the Rust twin. Here the extra routes are
the Python ones: pandas and Arrow inputs, whose scores are sampled past the
quality reservoir like a file's, dict and CSV bytes inputs, whose scores are
exact, baselines read back from ``to_json()`` and from a flat ``to_dict()``
summary, and the ``python -m dataprof.check`` flag.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import dataprof as dp
import pytest
from dataprof.check import __main__ as entrypoint

pa = pytest.importorskip("pyarrow")
pd = pytest.importorskip("pandas")

ROWS = 25_000


def _rows(bad_every: int) -> list[tuple[int, str, str, str]]:
    """Rows past the 10,000-value reservoir. Every ``bad_every``-th row
    breaks three columns at once."""
    rows = []
    for i in range(ROWS):
        if i % bad_every == 0:
            rows.append((i, "n/a", "x", "notadate"))
        else:
            rows.append(
                (
                    i,
                    f"{(i * 37) % 1000}.{i % 100:02d}",
                    f"{i % 100000:05d}",
                    f"2025-01-{1 + i % 28:02d}",
                )
            )
    return rows


def _csv(bad_every: int) -> bytes:
    lines = ["id,amount,code,when", *(",".join(map(str, row)) for row in _rows(bad_every))]
    return ("\n".join(lines) + "\n").encode()


def _columns(bad_every: int) -> dict[str, list[object]]:
    rows = _rows(bad_every)
    return {
        name: [row[i] for row in rows] for i, name in enumerate(["id", "amount", "code", "when"])
    }


#: Routes whose scores past the reservoir are sampled, and so bounded.
SAMPLED: dict[str, Callable[[int, Path], dp.ProfileReport]] = {
    "file": lambda bad, tmp: _file(bad, tmp),
    "pandas": lambda bad, _: dp.profile(pd.DataFrame(_columns(bad))),
    "arrow": lambda bad, _: dp.profile(pa.table(_columns(bad))),
}

BASELINE_FORMS: dict[str, Callable[[dp.ProfileReport], dp.ProfileReport]] = {
    "live": lambda report: report,
    "to_json": lambda report: dp.ProfileReport.from_json(report.to_json()),
    "to_dict": lambda report: dp.ProfileReport.from_dict(report.to_dict()),
}


def _file(bad_every: int, tmp: Path) -> dp.ProfileReport:
    path = tmp / f"bad_{bad_every}.csv"
    path.write_bytes(_csv(bad_every))
    return dp.profile(str(path))


def _status(result: dp.QualityGateResult, code: str) -> str:
    (check,) = [check for check in result.checks if check.code == code]
    if check.reason is None:
        return check.status
    return f"{check.reason['reason']}:{check.reason.get('gap')}"


INCONCLUSIVE = "evidence_incomplete:quality_sampled"

#: (case, bad_every of the report, allowance, overall, consistency)
CASES = [
    ("unchanged, no drop allowed", 50, 0, INCONCLUSIVE, INCONCLUSIVE),
    ("unchanged, 5 points", 50, 5, "passed", "passed"),
    ("a tenth of the rows broken, 5 points", 5, 5, "failed", "failed"),
    ("a tenth of the rows broken, 30 points", 5, 30, "passed", "passed"),
]


def test_a_sampled_score_drop_is_decided_on_both_sides_intervals(tmp_path: Path):
    wrong = []
    for route, make in SAMPLED.items():
        baseline = make(50, tmp_path)
        assert baseline.quality_score_bounds is not None, route
        reports = {bad: make(bad, tmp_path) for bad in {case[1] for case in CASES}}
        for form, read in BASELINE_FORMS.items():
            side = read(baseline)
            for case, bad, points, overall, consistency in CASES:
                result = reports[bad].check(
                    baseline=side,
                    max_quality_score_drop=points,
                    max_dimension_score_drops={"consistency": points},
                )
                got = (
                    _status(result, "max_quality_score_drop"),
                    _status(result, "max_dimension_score_drop"),
                )
                if got != (overall, consistency):
                    wrong.append(
                        f"{route}/{form}/{case}: {got}, expected ({overall}, {consistency})"
                    )
    assert not wrong, "\n".join(wrong)


def test_the_result_carries_the_intervals_a_drop_was_decided_on(tmp_path: Path):
    sampled = _file(50, tmp_path)
    exact = dp.profile(_csv(50), format="csv")
    assert exact.quality_score_bounds is None
    wrong = []
    for case, report, baseline, current_bounds, baseline_bounds in [
        ("both sampled", sampled, sampled, True, True),
        ("exact baseline", sampled, exact, True, False),
        ("exact report", exact, sampled, False, True),
    ]:
        for form, read in BASELINE_FORMS.items():
            result = report.check(baseline=read(baseline), max_quality_score_drop=50)
            (check,) = [c for c in result.checks if c.code == "max_quality_score_drop"]
            assert check.baseline is not None
            got = (check.status, check.bounds is not None, "bounds" in check.baseline)
            if got != ("passed", current_bounds, baseline_bounds):
                wrong.append(f"{case}/{form}: {got}")
            document = check.to_dict()
            if ("bounds" in document) != current_bounds:
                wrong.append(f"{case}/{form}: to_dict bounds")
    assert not wrong, "\n".join(wrong)


def test_only_a_full_source_drop_over_fully_read_sources_reads_the_intervals(tmp_path: Path):
    baseline = _file(50, tmp_path)
    path = tmp_path / "today.csv"
    path.write_bytes(_csv(50))
    capped = dp.profile_file(str(path), max_rows=20_000)
    wrong = []
    for form, read in BASELINE_FORMS.items():
        side = read(baseline)
        got = _status(
            capped.check(baseline=side, max_quality_score_drop=5), "max_quality_score_drop"
        )
        if got != "evidence_incomplete:truncated":
            wrong.append(f"{form}/capped: {got}")
        observed = baseline.check(baseline=side, max_quality_score_drop=0, scope="observed")
        (check,) = [c for c in observed.checks if c.code == "max_quality_score_drop"]
        if check.status != "passed" or check.bounds is not None:
            wrong.append(f"{form}/observed: {check}")
    assert not wrong, "\n".join(wrong)


def test_the_check_entrypoint_decides_a_sampled_drop(tmp_path: Path):
    baseline = tmp_path / "baseline.json"
    _file(50, tmp_path).save(baseline)
    source = tmp_path / "today.csv"
    common = [str(source), "--baseline", str(baseline)]

    source.write_bytes(_csv(50))
    assert entrypoint.main([*common, "--max-quality-drop", "5"]) == 0
    assert entrypoint.main([*common, "--max-quality-drop", "0"]) == 2
    source.write_bytes(_csv(5))
    assert entrypoint.main([*common, "--max-quality-drop", "5"]) == 1
