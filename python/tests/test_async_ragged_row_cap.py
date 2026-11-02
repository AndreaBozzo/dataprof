"""``ragged_row_count`` counts the rows a profile consumed, on every route (#1203).

The async CSV reader parses ahead of the row cap, so counting there included
ragged rows past it. ``tests/async_ragged_row_cap.rs`` is the Rust twin.
"""

from __future__ import annotations

import asyncio
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

if not dp.capabilities().async_streaming:
    pytest.skip("Async streaming not compiled.", allow_module_level=True)

#: ``(case, cap, source, ragged rows within the cap)``.
CASES = [
    (
        "ragged rows only past the cap",
        1,
        b"name,age,city\nAlice,25,Rome\nBob,30\nCarl,40,Milan,x\n",
        0,
    ),
    (
        "ragged rows on both sides of the cap",
        2,
        b"name,age,city\nAlice,25\nBob,30,Rome\nCarl,40\nDan,50,Turin,x\n",
        1,
    ),
    ("cap of zero", 0, b"name,age,city\nAlice,25\nBob,30,Rome\n", 0),
    (
        "cap the file never reaches",
        10,
        b"name,age,city\nAlice,25\nBob,30,Rome\nCarl,40,Milan,x\n",
        2,
    ),
]


def test_async_csv_counts_no_ragged_row_past_the_cap(tmp_path: Path) -> None:
    wrong = []
    for case, cap, data, expected in CASES:
        path = tmp_path / "ragged.csv"
        path.write_bytes(data)
        counts = {
            engine: dp.profile(path, engine=engine, max_rows=cap).ragged_row_count
            for engine in ("auto", "incremental", "columnar")
        }
        counts["async file"] = asyncio.run(profile_file(path, max_rows=cap)).ragged_row_count
        counts["async bytes"] = asyncio.run(
            profile_bytes(data, format="csv", max_rows=cap)
        ).ragged_row_count
        wrong += [
            f"{route}, {case}: {count}, expected {expected}"
            for route, count in counts.items()
            if count != expected
        ]
    assert not wrong, "\n".join(wrong)
