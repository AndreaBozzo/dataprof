"""Timezone-aware Python datetimes are dates (#1335).

`tests/spaced_offset_datetimes.rs` is the Rust twin, over the file routes.
Here the routes are the in-memory ones: a dict of columns and a list of
records render each cell with ``str()``, which writes an aware datetime as
``2026-06-01 12:00:00+00:00``, and so does a pandas ``Timestamp``.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from typing import Any
from zoneinfo import ZoneInfo

import dataprof as dp
import pytest

pd = pytest.importorskip("pandas")

UTC = dt.UTC
EAST = dt.timezone(dt.timedelta(hours=2))
WEST = dt.timezone(dt.timedelta(hours=-5))

#: (case, two cells on the UTC dates 2026-05-31 and 2026-06-01)
CASES: list[tuple[str, list[Any]]] = [
    (
        "the issue's UTC datetime",
        [dt.datetime(2026, 5, 31, 12, tzinfo=UTC), dt.datetime(2026, 6, 1, 12, tzinfo=UTC)],
    ),
    (
        "+02:00, carried back across midnight",
        [dt.datetime(2026, 6, 1, 1, tzinfo=EAST), dt.datetime(2026, 6, 2, 1, tzinfo=EAST)],
    ),
    (
        "-05:00 with microseconds, carried forward",
        [
            dt.datetime(2026, 5, 30, 20, 0, 0, 123456, tzinfo=WEST),
            dt.datetime(2026, 5, 31, 21, tzinfo=WEST),
        ],
    ),
    (
        "zoneinfo",
        [
            dt.datetime(2026, 5, 31, 12, tzinfo=ZoneInfo("Europe/Rome")),
            dt.datetime(2026, 6, 1, 12, tzinfo=ZoneInfo("Europe/Rome")),
        ],
    ),
    (
        "naive with microseconds",
        [dt.datetime(2026, 5, 31, 12, 0, 0, 5), dt.datetime(2026, 6, 1, 12, 0, 0, 5)],
    ),
    (
        "pandas Timestamp with nanoseconds",
        [
            pd.Timestamp("2026-05-31 12:00:00.000000001", tz="UTC"),
            pd.Timestamp("2026-06-01 12:00", tz="UTC"),
        ],
    ),
]

REF = "2026-06-01"

ROUTES: dict[str, Callable[[list[Any]], dp.ProfileReport]] = {
    "dict": lambda cells: dp.profile({"published_at": cells}, reference_date=REF),
    "list of records": lambda cells: dp.profile(
        [{"published_at": cell} for cell in cells], reference_date=REF
    ),
    # A text cell beside the datetimes makes pyarrow refuse the column, so
    # each cell is rendered with `str()`; "NA" is then read as a null. With
    # `None` instead, pyarrow would convert the column to a timestamp itself.
    "pandas mixed object": lambda cells: dp.profile(
        pd.DataFrame({"published_at": pd.Series([*cells, "NA"], dtype=object)}),
        reference_date=REF,
    ),
}


def test_aware_datetimes_are_dates_on_every_python_route():
    checks: dict[str, Any] = {
        "max_date_age": {"published_at": 0},
        "max_date_gap": {"published_at": 1},
    }
    wrong = []
    for case, cells in CASES:
        for route, profile in ROUTES.items():
            report = profile(cells)
            column = report.to_dict()["columns"][0]
            if column["data_type"] != "date":
                wrong.append(f"{case} [{route}]: typed {column['data_type']}")
                continue
            if column["invalid_count"] != 0:
                wrong.append(f"{case} [{route}]: {column['invalid_count']} invalid")
            # Age 0 puts the newest UTC date on the reference date, 2026-06-01;
            # a gap of 1 between two values puts the oldest on 2026-05-31.
            result = report.check(**checks)
            got = [(check.status, check.observed) for check in result.checks]
            if got != [("passed", 0), ("passed", 1)]:
                wrong.append(f"{case} [{route}]: {got}")
    assert not wrong, "\n".join(wrong)
