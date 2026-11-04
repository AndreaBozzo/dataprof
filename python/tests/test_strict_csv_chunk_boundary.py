"""Strict CSV parsing refuses a ragged record wherever a chunk boundary falls (#1202).

The incremental engine reads a file in chunks, and the ``csv`` crate's strict
mode compares a record only with the previous record of the same chunk, so a
ragged record that started a chunk was profiled. Flexible parsing still
recovers and counts every ragged row on the same chunk sizes.

``tests/strict_csv_chunk_boundary.rs`` is the Rust twin.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

try:
    import dataprof as dp
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

_RUN = (
    b"name,age,city\n"
    + b"".join(f"person{i},{i},Rome\n".encode() for i in range(40))
    + b"".join(f"person{i},{i}\n".encode() for i in range(40))
)

#: ``(case, source, ragged rows)``: each source holds ragged records.
SOURCES = [
    ("short record", b"name,age,city\nAlice,25,Rome\nBob,30\nCarl,40,Milan\n", 1),
    ("long record", b"name,age,city\nAlice,25,Rome\nBob,30,Milan,extra\nCarl,40,Milan\n", 1),
    ("ragged last record", b"name,age,city\nAlice,25,Rome\nBob,30,Milan\nCarl,40\n", 1),
    # A chunk that starts at the first two-field record holds nothing the
    # previous-record rule can refuse.
    ("run of one wrong width", _RUN, 40),
]

#: One-record chunks, the issue's 20 bytes, and the adaptive default, which
#: reads these small files in one chunk.
CHUNK_SIZES: list[int | None] = [1, 20, None]


def _strict_routes(path: Path) -> dict[str, Callable[[], dp.ProfileReport]]:
    routes: dict[str, Callable[[], dp.ProfileReport]] = {
        f"incremental, chunk_size={size}": (
            lambda size=size: dp.profile(
                path, engine="incremental", csv_flexible=False, chunk_size=size
            )
        )
        for size in CHUNK_SIZES
    }
    for engine in ("auto", "columnar"):
        routes[engine] = lambda engine=engine: dp.profile(path, engine=engine, csv_flexible=False)
    return routes


def test_strict_parsing_refuses_a_ragged_record_on_every_chunk_size(tmp_path: Path) -> None:
    wrong = []
    for case, data, _ in SOURCES:
        path = tmp_path / "ragged.csv"
        path.write_bytes(data)
        for route, run in _strict_routes(path).items():
            try:
                report = run()
            except ValueError as error:
                if "fields" not in str(error):
                    wrong.append(f"{route}, {case}: {error}")
                continue
            wrong.append(
                f"{route}, {case}: profiled {report.rows} rows, {report.ragged_row_count} ragged"
            )
    assert not wrong, "\n".join(wrong)


def test_flexible_parsing_counts_every_ragged_record_on_every_chunk_size(
    tmp_path: Path,
) -> None:
    wrong = []
    for case, data, ragged in SOURCES:
        path = tmp_path / "ragged.csv"
        path.write_bytes(data)
        rows = data.count(b"\n") - 1
        for size in CHUNK_SIZES:
            report = dp.profile(path, engine="incremental", csv_flexible=True, chunk_size=size)
            if (report.rows, report.ragged_row_count) != (rows, ragged):
                wrong.append(
                    f"chunk_size={size}, {case}: rows {report.rows}, "
                    f"ragged {report.ragged_row_count}"
                )
    assert not wrong, "\n".join(wrong)
