"""``max_rows`` bounds the analyzed population under strict CSV parsing (#757).

A ragged record past the cap is not part of the profile, so
``csv_flexible=False`` must not fail over it on any route. Synchronous bytes
have no flexible mode and refuse every ragged row they read, so the cap bounds
what they read too, for a quote they never see closed as for a ragged row. A
ragged record inside the cap still fails everywhere.

``tests/strict_csv_row_cap.rs`` is the Rust twin.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from pathlib import Path

import pytest

try:
    import dataprof as dp
    from dataprof.asyncio import profile_bytes, profile_file
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

#: ``(case, cap, source)``: the cap admits only well-formed rows, and a ragged
#: record or one that opens a quote it never closes follows them.
PAST_THE_CAP = [
    ("short record", 1, b"name,age,city\nAlice,25,Rome\nBob,30\n"),
    ("long record", 1, b"name,age,city\nAlice,25,Rome\nBob,30,Milan,extra\n"),
    (
        "ragged record two past the cap",
        1,
        b"name,age,city\nAlice,25,Rome\nBob,30,Milan\nCarl,40\n",
    ),
    ("cap of zero, ragged first record", 0, b"name,age,city\nBob,30\nCarl,40,Milan\n"),
    (
        "unclosed quote past the cap",
        1,
        b'name,age,city\nAlice,25,Rome\nBob,"30,Milan\nCarl,40,Turin\n',
    ),
]

#: The second row is ragged and a cap of two admits it.
INSIDE_THE_CAP = b"name,age,city\nAlice,25,Rome\nBob,30\nCarl,40,Milan\n"


def _routes(path: Path, data: bytes, cap: int) -> dict[str, Callable[[], dp.ProfileReport]]:
    routes: dict[str, Callable[[], dp.ProfileReport]] = {
        engine: (
            lambda engine=engine: dp.profile(path, engine=engine, csv_flexible=False, max_rows=cap)
        )
        for engine in ("auto", "incremental", "columnar")
    }
    routes["async file"] = lambda: asyncio.run(profile_file(path, csv_flexible=False, max_rows=cap))
    routes["async bytes"] = lambda: asyncio.run(
        profile_bytes(data, format="csv", csv_flexible=False, max_rows=cap)
    )
    # Always strict: it refuses csv_flexible=True rather than ignore it.
    routes["bytes"] = lambda: dp.profile(data, format="csv", max_rows=cap)
    return routes


def test_strict_parsing_does_not_judge_a_record_past_the_row_cap(tmp_path: Path) -> None:
    wrong = []
    for case, cap, data in PAST_THE_CAP:
        path = tmp_path / "capped.csv"
        path.write_bytes(data)
        for route, run in _routes(path, data, cap).items():
            try:
                report = run()
            except Exception as error:  # noqa: BLE001 - every failure is a finding
                wrong.append(f"{route}, {case}: {error}")
                continue
            shape = (report.rows, report.ragged_row_count, report.truncation_reason is not None)
            if shape != (cap, 0, True):
                wrong.append(f"{route}, {case}: rows, ragged, truncated = {shape}")
    assert not wrong, "\n".join(wrong)


def test_strict_parsing_still_rejects_a_ragged_record_inside_the_row_cap(
    tmp_path: Path,
) -> None:
    path = tmp_path / "ragged.csv"
    path.write_bytes(INSIDE_THE_CAP)
    wrong = []
    for route, run in _routes(path, INSIDE_THE_CAP, 2).items():
        try:
            report = run()
        except (RuntimeError, ValueError):
            continue
        wrong.append(f"{route}: profiled {report.rows} rows")
    assert not wrong, "\n".join(wrong)


@pytest.mark.parametrize("cap", [None, 2, 5])
def test_bytes_still_refuse_an_unclosed_quote_the_cap_reaches(cap: int | None) -> None:
    # The row that opens the quote swallows the rest, so it is ragged as well;
    # the quote is the cause, and the message names it.
    data = b'name,age,city\nAlice,25,Rome\nBob,"30,Milan\nCarl,40,Turin\n'
    with pytest.raises(ValueError, match="never closed"):
        dp.profile(data, format="csv", max_rows=cap)
