"""Delimiter detection reads each file the way its header and records say.

Every reader holds a record to the header's width. Detection that chose a
delimiter the header did not split, scored a CR-only file as one header
record, or counted a trailing empty field against its delimiter, made the
readers cut or pad the rows: composite keys ``123|456`` and ``123|789`` became
two ``123`` and ``max_duplicate_rows=0`` failed (#1092, #1097), and a semicolon
export with trailing ``;`` gained an invented null column (#1096). Synchronous
bytes detected with ``csv.Sniffer`` instead, which fell back to comma on a long
first row and hid a null from ``max_null_percentage=0`` (#1095).

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

#: label -> (bytes, expected columns as (name, null_count), duplicate rows)
CASES: dict[str, tuple[bytes, list[tuple[str, int]], int]] = {
    "composite keys, LF": (b"key\n123|456\n123|789\n", [("key", 0)], 0),
    "composite keys, CR": (b"key\r123|456\r123|789\r", [("key", 0)], 0),
    "semicolon rows with a trailing empty field": (
        b"id;amount,EUR\n1;10,50;\n2;20;\n",
        [("id", 0), ("amount,EUR", 0)],
        0,
    ),
    "semicolon, CR": (b"id;value\r1;10\r2;\r", [("id", 0), ("value", 1)], 0),
    "semicolon, long first row": (
        ("payload;amount\n" + "x" * 8192 + ";\n").encode(),
        [("payload", 0), ("amount", 1)],
        0,
    ),
}

#: Synchronous bytes refuse a ragged row rather than cut it, on every release.
STRICT_BYTES = {"semicolon rows with a trailing empty field"}


def _shape(report: dp.ProfileReport) -> dict[str, Any]:
    doc = report.to_dict()
    return {
        "columns": [(c["name"], c["null_count"]) for c in doc["columns"]],
        "duplicate_rows": doc["quality"]["uniqueness"]["duplicate_rows"],
    }


def test_detected_delimiter_keeps_every_value(tmp_path: Path) -> None:
    wrong = {}
    for label, (data, columns, duplicates) in CASES.items():
        path = tmp_path / "probe.csv"
        path.write_bytes(data)
        routes = {
            "async bytes": _shape(asyncio.run(profile_bytes(data, format="csv"))),
            "async file": _shape(asyncio.run(profile_file(path))),
        }
        if label not in STRICT_BYTES:
            routes["bytes"] = _shape(dp.profile(data, format="csv"))
        for engine in ("auto", "incremental", "columnar"):
            routes[engine] = _shape(dp.profile(path, engine=engine))

        expected = {"columns": columns, "duplicate_rows": duplicates}
        wrong.update(
            {f"[{label}] {route}": shape for route, shape in routes.items() if shape != expected}
        )
    assert not wrong, wrong


def test_bytes_gate_sees_the_null_a_file_sees() -> None:
    data = ("payload;amount\n" + "x" * 8192 + ";\n").encode()
    assert dp.profile(data, format="csv").check(max_null_percentage=0).verdict == "fail"
