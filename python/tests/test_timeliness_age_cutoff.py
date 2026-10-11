"""Whole-date freshness cutoffs. Rust twin: tests/timeliness_age_cutoff.rs."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

from _profile_routes import _routes


def test_every_route_compares_whole_dates_for_staleness(tmp_path: Path):
    today = datetime.now(timezone.utc).date()
    cutoff = today - timedelta(days=int(5.0 * 365.2425))
    values = [
        str(cutoff - timedelta(days=1)),
        str(cutoff),
        str(cutoff + timedelta(days=1)),
        str(today),
    ]
    wrong = []
    for route, run in _routes(tmp_path).items():
        report = run("date-cutoff", values).to_dict()
        metrics = report["quality"]["timeliness"]
        if metrics["date_values_checked"] != 4 or metrics["stale_data_ratio"] != 25.0:
            wrong.append(f"[{route}] {metrics}")
    assert not wrong, "\n".join(wrong)
