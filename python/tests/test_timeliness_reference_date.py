"""A fixed timeliness reference date, recorded in the report (#903).

Timeliness compared dates with today's, so an unchanged file changed score as
the calendar moved. ``reference_date=`` fixes the date on every input route,
and the report records the clock it used under ``quality.timeliness["clock"]``,
which ``compare()`` lists under ``configuration``.

``tests/timeliness_reference_date.rs`` is the Rust twin.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

try:
    import dataprof as dp
    from dataprof.asyncio import (
        _HAS_ASYNC,
        profile_bytes as profile_bytes_async,
        profile_file as profile_file_async,
    )
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

# Five years before the fixed date is 2021-06-10 (1826 days, one of them
# 29 February 2024), so the first value is the one stale date and the last is
# the one future date.
DATES = ["2021-06-09", "2021-06-10", "2026-06-10", "2026-06-11"]
FIXED = dt.date(2026, 6, 10)
STALE_RATIO = 25.0
FUTURE_DATES = 1
FIXED_CLOCK = {
    "mode": "fixed",
    "reference_date": "2026-06-10",
    "timezone": "UTC",
    "max_data_age_years": 5.0,
}
CSV = "id,updated_at\n" + "".join(f"{i},{d}\n" for i, d in enumerate(DATES))


def _routes(tmp_path: Path) -> dict[str, Callable[..., Any]]:
    csv = tmp_path / "t.csv"
    csv.write_text(CSV)
    records = [{"id": i, "updated_at": d} for i, d in enumerate(DATES)]
    as_json = tmp_path / "t.json"
    as_json.write_text(json.dumps(records))
    jsonl = tmp_path / "t.jsonl"
    jsonl.write_text("".join(json.dumps(r) + "\n" for r in records))
    columns = {"id": list(range(len(DATES))), "updated_at": DATES}

    routes: dict[str, Callable[..., Any]] = {
        "csv auto": lambda **k: dp.profile(csv, **k),
        "csv incremental": lambda **k: dp.profile(csv, engine="incremental", **k),
        "csv columnar": lambda **k: dp.profile(csv, engine="columnar", **k),
        "json": lambda **k: dp.profile(as_json, **k),
        "jsonl": lambda **k: dp.profile(jsonl, **k),
        "dict": lambda **k: dp.profile(columns, **k),
        "csv bytes": lambda **k: dp.profile(CSV.encode(), format="csv", **k),
        "Profiler": lambda **k: _profiler(csv, **k),
    }
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        pass
    else:
        parquet = tmp_path / "t.parquet"
        pq.write_table(pa.table(columns), parquet)
        routes["parquet"] = lambda **k: dp.profile(parquet, **k)
        routes["arrow date32"] = lambda **k: dp.profile(
            pa.table({"updated_at": pa.array([dt.date.fromisoformat(d) for d in DATES])}), **k
        )
    if _HAS_ASYNC:
        routes["async bytes"] = lambda **k: asyncio.run(
            profile_bytes_async(CSV.encode(), format="csv", **k)
        )
        routes["async file"] = lambda **k: asyncio.run(profile_file_async(csv, **k))
    return routes


def _profiler(path: Path, reference_date: Any = None, temporal_columns: Any = None) -> Any:
    profiler = dp.Profiler().temporal_columns(temporal_columns)
    if reference_date is not None:
        profiler = profiler.reference_date(reference_date)
    return profiler.profile(path)


def test_every_route_measures_against_a_fixed_date_and_records_it(tmp_path: Path) -> None:
    wrong = []
    for name, run in _routes(tmp_path).items():
        for reference_date in (FIXED, "2026-06-10"):
            report = run(reference_date=reference_date, temporal_columns=["updated_at"])
            timeliness = report.quality.timeliness
            if timeliness is None:
                wrong.append((name, reference_date, "timeliness not assessed"))
                continue
            if timeliness.get("clock") != FIXED_CLOCK:
                wrong.append((name, reference_date, timeliness.get("clock")))
            if (
                timeliness["stale_data_ratio"] != STALE_RATIO
                or timeliness["future_dates_count"] != FUTURE_DATES
            ):
                wrong.append((name, reference_date, timeliness))
            summary = report.to_dict()["quality"]["timeliness"].get("clock")
            document = json.loads(report.to_json())["quality"]["metrics"]["timeliness"].get("clock")
            restored = dp.ProfileReport.from_dict(report.to_dict()).to_dict()
            if not (
                summary == document == restored["quality"]["timeliness"].get("clock") == FIXED_CLOCK
            ):
                wrong.append((name, reference_date, summary, document, restored))
    assert wrong == []


def test_without_a_date_every_route_records_todays_rolling_clock(tmp_path: Path) -> None:
    wrong = []
    for name, run in _routes(tmp_path).items():
        before = dt.datetime.now(dt.UTC).date()
        clock = run(temporal_columns=["updated_at"]).quality.timeliness["clock"]
        after = dt.datetime.now(dt.UTC).date()
        if clock["mode"] != "rolling" or not (
            before <= dt.date.fromisoformat(clock["reference_date"]) <= after
        ):
            wrong.append((name, clock))
    assert wrong == []


def test_compare_lists_the_clock_from_either_dialect(tmp_path: Path) -> None:
    path = tmp_path / "t.csv"
    path.write_text(CSV)
    a = dp.profile(path, reference_date=FIXED, temporal_columns=["updated_at"])
    b = dp.profile(path, reference_date="2026-06-09", temporal_columns=["updated_at"])

    native = a.compare(b)["configuration"]["timeliness_clock"]
    assert native["a"] == FIXED_CLOCK
    assert native["b"]["reference_date"] == "2026-06-09"
    assert native["same"] is False

    flat_a = dp.ProfileReport.from_dict(a.to_dict())
    flat_b = dp.ProfileReport.from_dict(b.to_dict())
    assert flat_a.compare(flat_b)["configuration"]["timeliness_clock"]["same"] is False
    assert flat_a.compare(a)["configuration"]["timeliness_clock"]["same"] is True

    old = a.to_dict()
    del old["quality"]["timeliness"]["clock"]
    unrecorded = dp.ProfileReport.from_dict(old).compare(a)["configuration"]["timeliness_clock"]
    assert unrecorded["a"] is None
    assert unrecorded["same"] is None


def test_profiler_config_reads_back_the_date() -> None:
    assert dp.ProfilerConfig(reference_date="2026-06-10").reference_date == FIXED
    assert dp.ProfilerConfig(reference_date=FIXED).reference_date == FIXED
    assert dp.ProfilerConfig().reference_date is None


@pytest.mark.parametrize(
    ("value", "error"),
    [
        ("2026-6-10", ValueError),
        ("+2026-06-10", ValueError),
        ("2026-06-10T00:00", ValueError),
        ("2026-02-30", ValueError),
        ("", ValueError),
        (dt.datetime(2026, 6, 10, tzinfo=dt.UTC), TypeError),
        (20260610, TypeError),
    ],
)
def test_a_value_that_is_not_one_calendar_date_is_refused(
    value: Any, error: type[Exception]
) -> None:
    with pytest.raises(error, match="reference_date"):
        dp.ProfilerConfig(reference_date=value)


@pytest.mark.parametrize("clock", [None, "2026-06-10", {"mode": "weekly"}])
def test_a_loaded_summary_refuses_a_malformed_clock(tmp_path: Path, clock: Any) -> None:
    path = tmp_path / "t.csv"
    path.write_text(CSV)
    summary = dp.profile(path, reference_date=FIXED, temporal_columns=["updated_at"]).to_dict()
    summary["quality"]["timeliness"]["clock"] = clock
    with pytest.raises(ValueError, match="quality.timeliness.clock"):
        dp.ProfileReport.from_dict(summary)
