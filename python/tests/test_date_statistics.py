"""Date statistics over every value read, not the retained sample (#1297).

`tests/date_statistics.rs` is the Rust twin; it also covers the standard,
auto, columnar, JSON array and async routes. Here the extra routes are the
Python ones: a file path per engine, CSV bytes, a dict, pandas and Arrow
inputs, and a report read back from ``to_json()``.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import dataprof as dp
import pytest

pa = pytest.importorskip("pyarrow")
pd = pytest.importorskip("pandas")
pq = pytest.importorskip("pyarrow.parquet")

ROWS = 40_000
COLUMNS = ["published_at", "us_date", "logged_at"]
# Rows past the sample, each holding the one value that moves a statistic.
NEWEST_ROW, OLDEST_ROW, MONTH_FIRST_ROW, LATE_HOUR_ROW = 25_000, 33_000, 31_000, 37_000


def _row(i: int) -> list[str]:
    if i == NEWEST_ROW:
        published_at = "2026-09-29"
    elif i == OLDEST_ROW:
        published_at = "2019-03-04"
    else:
        published_at = str(dt.date(2020, 1, 1) + dt.timedelta(days=i % 500))
    # Day and month both at most 12, so every value but one parses both ways.
    us_date = (
        "12/31/2024" if i == MONTH_FIRST_ROW else f"{i % 12 + 1:02}/{i // 12 % 12 + 1:02}/2024"
    )
    logged_at = "2021-05-05T23:00:00" if i == LATE_HOUR_ROW else f"2021-05-{i % 28 + 1:02}T10:00:00"
    return [published_at, us_date, logged_at]


ROWS_DATA = [_row(i) for i in range(ROWS)]
DATA = {name: [row[index] for row in ROWS_DATA] for index, name in enumerate(COLUMNS)}


def _csv() -> bytes:
    lines = [",".join(COLUMNS)] + [",".join(row) for row in ROWS_DATA]
    return ("\n".join(lines) + "\n").encode()


def _write(tmp_path: Path, name: str, data: bytes) -> str:
    path = tmp_path / name
    path.write_bytes(data)
    return str(path)


def _jsonl() -> bytes:
    return "".join(
        json.dumps(dict(zip(COLUMNS, row, strict=True))) + "\n" for row in ROWS_DATA
    ).encode()


def _parquet(tmp_path: Path, typed_dates: bool) -> str:
    columns: dict[str, Any] = dict(DATA)
    if typed_dates:
        columns["published_at"] = pa.array([dt.date.fromisoformat(v) for v in DATA["published_at"]])
    path = tmp_path / ("typed.parquet" if typed_dates else "text.parquet")
    pq.write_table(pa.table(columns), path)
    return str(path)


ROUTES: dict[str, Callable[[Path], dp.ProfileReport]] = {
    "csv path": lambda tmp: dp.profile(_write(tmp, "d.csv", _csv())),
    "csv path incremental": lambda tmp: dp.profile(
        _write(tmp, "d.csv", _csv()), engine="incremental"
    ),
    "csv path columnar": lambda tmp: dp.profile(_write(tmp, "d.csv", _csv()), engine="columnar"),
    "jsonl path": lambda tmp: dp.profile(_write(tmp, "d.jsonl", _jsonl())),
    "parquet path": lambda tmp: dp.profile(_parquet(tmp, False)),
    "parquet date32 path": lambda tmp: dp.profile(_parquet(tmp, True)),
    "csv bytes": lambda tmp: dp.profile(_csv(), format="csv"),
    "dict": lambda tmp: dp.profile(DATA),
    "pandas": lambda tmp: dp.profile(pd.DataFrame(DATA)),
    "arrow strings": lambda tmp: dp.profile(pa.table(DATA)),
    "to_json": lambda tmp: dp.ProfileReport.from_json(
        dp.profile(_write(tmp, "d.csv", _csv())).to_json()
    ),
}


def _date_stats(report: dp.ProfileReport) -> dict[str, dict[str, Any]]:
    document = json.loads(report.to_json())
    return {
        column["name"]: column["stats"].get("DateTime")
        for column in document["column_profiles"]
        if column["name"] in COLUMNS
    }


def test_the_in_memory_oracle_holds_the_values_past_the_sample():
    stats = _date_stats(dp.profile(DATA))
    assert (stats["published_at"]["min_datetime"], stats["published_at"]["max_datetime"]) == (
        "2019-03-04",
        "2026-09-29",
    )
    assert stats["published_at"]["year_distribution"]["2026"] == 1
    assert stats["us_date"]["slash_date_order"] == "month_first"
    assert stats["logged_at"]["hour_distribution"]["23"] == 1


def test_every_python_route_reports_the_date_statistics_of_every_value(tmp_path: Path):
    expected = _date_stats(dp.profile(DATA))
    wrong = []
    for label, route in ROUTES.items():
        report = route(tmp_path)
        assert report.metric_semantics is not None, label
        assert report.metric_semantics["date_statistics"] == "every_value", label
        got = _date_stats(report)
        for name in COLUMNS:
            if got.get(name) is None:
                wrong.append(f"[{label}] {name}: no date statistics")
                continue
            for field in sorted(set(expected[name]) | set(got[name])):
                if got[name].get(field) != expected[name].get(field):
                    wrong.append(f"[{label}] {name}.{field}")
    assert not wrong, f"{len(wrong)} wrong:\n" + "\n".join(wrong)


def test_the_file_routes_hold_only_a_sample(tmp_path: Path):
    # Without this the guard above could pass on a fixture every route held
    # in full, and guard nothing.
    report = dp.profile(_write(tmp_path, "d.csv", _csv()))
    document = json.loads(report.to_json())
    published_at = next(c for c in document["column_profiles"] if c["name"] == "published_at")
    assert sum(published_at["type_homogeneity"].values()) < ROWS
