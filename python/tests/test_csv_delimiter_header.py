"""Delimiter detection never chooses a delimiter the header does not use.

Every reader cuts a record to the header's width. When detection picked a
delimiter that split the data rows but left the header as one field, a
one-column file of composite keys was profiled as the keys' first parts:
``123|456`` and ``123|789`` became two ``123``, and ``max_duplicate_rows=0``
failed on a duplicate the input does not contain (#1092).

``tests/csv_delimiter_header.rs`` is the Rust twin.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

try:
    import dataprof as dp
    from dataprof.asyncio import profile_bytes, profile_file
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

DATA = b"key\n123|456\n123|789\n"


def _shape(report: dp.ProfileReport) -> dict[str, Any]:
    doc = report.to_dict()
    column = doc["columns"][0]
    return {
        "columns": len(doc["columns"]),
        "data_type": column["data_type"],
        "unique_count": column["unique_count"],
        "duplicate_rows": doc["quality"]["uniqueness"]["duplicate_rows"],
        "verdict": report.check(max_duplicate_rows=0).verdict,
    }


def test_composite_keys_keep_their_full_value(tmp_path: Path) -> None:
    path = tmp_path / "keys.csv"
    path.write_bytes(DATA)

    routes = {
        "bytes": _shape(dp.profile(DATA, format="csv")),
        "async bytes": _shape(asyncio.run(profile_bytes(DATA, format="csv"))),
        "async file": _shape(asyncio.run(profile_file(path))),
    }
    for engine in ("auto", "incremental", "columnar"):
        routes[engine] = _shape(dp.profile(path, engine=engine))

    expected = {
        "columns": 1,
        "data_type": "string",
        "unique_count": 2,
        "duplicate_rows": 0,
        "verdict": "pass",
    }
    wrong = {route: shape for route, shape in routes.items() if shape != expected}
    assert not wrong, wrong
