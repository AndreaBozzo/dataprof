"""The baseline gate reads the timeliness clock (#1024).

The timeliness score counts dates later than a reference date and dates older
than a maximum age, so two reports measured against different clocks score
the same data differently. ``check(timeliness_clock=...)`` declares which
difference still measures the same thing: ``"fixed"`` (the default) needs the
same reference date, ``"rolling"`` lets two rolling clocks, each read on its
own run day, differ in date. Time zone and maximum age must match under
either, and a clock either side did not record leaves the rule unevaluated.

``tests/timeliness_clock_gate.rs`` is the Rust twin; this file also covers the
keyword, the command line flag and the policy file key.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

try:
    import dataprof as dp
    from dataprof.check import __main__ as entrypoint
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

DATES = ["2021-06-09", "2021-06-10", "2026-06-10", "2026-06-11"]
CSV = "id,updated_at\n" + "".join(f"{i},{d}\n" for i, d in enumerate(DATES))

#: A recorded clock: mode, reference date, maximum age; ``None`` is a report
#: written before 0.14, without one.
Clock = tuple[str, str, float] | None


def _with_clock(clock: Clock) -> dp.ProfileReport:
    """The CSV report with its recorded clock replaced. Only the record
    changes: the gate decides on what the reports recorded, and a rolling
    clock cannot be read on a chosen day."""
    document = json.loads(
        dp.profile({"id": list(range(len(DATES))), "updated_at": DATES}).to_json()
    )
    timeliness = document["quality"]["metrics"]["timeliness"]
    if clock is None:
        del timeliness["clock"]
    else:
        mode, reference_date, age = clock
        timeliness["clock"].update(mode=mode, reference_date=reference_date, max_data_age_years=age)
    return dp.ProfileReport.from_json(json.dumps(document))


F10: Clock = ("fixed", "2026-06-10", 5.0)
F11: Clock = ("fixed", "2026-06-11", 5.0)
R10: Clock = ("rolling", "2026-06-10", 5.0)
R11: Clock = ("rolling", "2026-06-11", 5.0)

CASES: list[tuple[str, str, Clock, Clock, str]] = [
    ("same fixed date", "fixed", F10, F10, "decided"),
    ("another fixed date", "fixed", F10, F11, "setting_changed"),
    ("same date, other mode", "fixed", R10, F10, "decided"),
    ("two rolling days, fixed rule", "fixed", R10, R11, "setting_changed"),
    ("two rolling days", "rolling", R10, R11, "decided"),
    ("fixed baseline, rolling report", "rolling", F10, R11, "setting_changed"),
    ("two fixed dates, rolling rule", "rolling", F10, F11, "setting_changed"),
    ("another age", "rolling", R10, ("rolling", "2026-06-11", 6.0), "setting_changed"),
    ("baseline before 0.14", "fixed", None, F10, "setting_unrecorded"),
    ("baseline before 0.14, rolling rule", "rolling", None, R11, "setting_unrecorded"),
]


def _outcome(check: Any) -> str:
    if check.status in ("passed", "failed"):
        return "decided"
    reason = check.reason or {}
    if reason.get("setting") != "timeliness_clock":
        return f"other: {reason}"
    return str(reason["reason"])


def test_the_score_rules_compare_clocks_as_the_policy_declares() -> None:
    wrong = []
    for name, rule, baseline_clock, report_clock, expected in CASES:
        baseline, report = _with_clock(baseline_clock), _with_clock(report_clock)
        # The baseline as save() writes it, and as a flat to_dict() summary.
        for route, side in (
            ("document", baseline),
            ("summary", dp.ProfileReport.from_dict(baseline.to_dict())),
        ):
            result = report.check(
                baseline=side,
                max_quality_score_drop=100,
                max_dimension_score_drops={"timeliness": 100, "completeness": 100},
                timeliness_clock=rule,
            )
            assert result.baseline is not None
            if result.baseline.get("timeliness_clock") != rule:
                wrong.append(f"{name} [{route}]: records {result.baseline.get('timeliness_clock')}")
            for check in result.checks:
                label = check.dimension or "overall"
                # Completeness counts nulls, whatever the clock.
                want = "decided" if check.dimension == "completeness" else expected
                got = _outcome(check)
                if got != want:
                    wrong.append(f"{name} [{route}] {label}: {got}, expected {want}")
    assert not wrong, "\n".join(wrong)


def test_without_timeliness_the_overall_rule_reads_no_clock() -> None:
    """Neither side has a timeliness score, so the overall score averages no
    clock, and neither the rule nor an absent clock leaves it unevaluated."""
    report = dp.profile({"id": [0, 1, 2], "name": ["a", "b", "c"]})
    assert "timeliness" not in report.to_dict()["quality"]
    for rule in ("fixed", "rolling"):
        result = report.check(baseline=report, max_quality_score_drop=0, timeliness_clock=rule)
        assert result.checks[0].status == "passed", (rule, result.checks[0].reason)


def test_the_default_is_the_fixed_rule() -> None:
    result = _with_clock(R11).check(baseline=_with_clock(R10), max_quality_score_drop=100)
    assert result.baseline is not None
    assert result.baseline["timeliness_clock"] == "fixed"
    assert _outcome(result.checks[0]) == "setting_changed"


def test_an_unknown_clock_is_a_policy_error() -> None:
    report = _with_clock(F10)
    with pytest.raises(ValueError, match="timeliness_clock must be one of"):
        report.check(baseline=report, max_quality_score_drop=1, timeliness_clock="daily")


@pytest.fixture
def daily(tmp_path: Path) -> tuple[Path, Path]:
    """A source the command line profiles with today's rolling clock, and a
    baseline saved by a run on another day."""
    source = tmp_path / "batch.csv"
    source.write_text(CSV)
    document = json.loads(dp.profile(source).to_json())
    clock = document["quality"]["metrics"]["timeliness"]["clock"]
    assert clock["mode"] == "rolling"
    clock["reference_date"] = "2001-01-01"
    baseline = tmp_path / "baseline.json"
    baseline.write_text(json.dumps(document))
    return source, baseline


def test_the_command_line_takes_the_clock_as_a_flag(
    daily: tuple[Path, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    source, baseline = daily
    common = [str(source), "--baseline", str(baseline), "--max-quality-drop", "100", "--json"]
    assert entrypoint.main(common) == 2
    output = json.loads(capsys.readouterr().out)
    assert output["checks"][0]["setting"] == "timeliness_clock"
    assert entrypoint.main([*common, "--timeliness-clock", "rolling"]) == 0
    assert json.loads(capsys.readouterr().out)["baseline"]["timeliness_clock"] == "rolling"


def test_a_policy_file_takes_the_clock_as_a_key(
    daily: tuple[Path, Path], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source, baseline = daily
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({"max_quality_score_drop": 100, "timeliness_clock": "rolling"}))
    args = [str(source), "--baseline", str(baseline), "--policy", str(policy), "--json"]
    assert entrypoint.main(args) == 0
    capsys.readouterr()
    # The flag replaces the file's key, as for every other policy key.
    assert entrypoint.main([*args, "--timeliness-clock", "fixed"]) == 2
