"""Delimiter detection reads each file the way its header and records say.

Every reader holds a record to the header's width. Detection that chose a
delimiter the header did not split, scored a CR-only file as one header
record, or counted a trailing empty field against its delimiter, made the
readers cut or pad the rows: composite keys ``123|456`` and ``123|789`` became
two ``123`` and ``max_duplicate_rows=0`` failed (#1092, #1097), and a semicolon
export with trailing ``;`` gained an invented null column (#1096). Synchronous
bytes detected with ``csv.Sniffer`` instead, which fell back to comma on a long
first row and hid a null from ``max_null_percentage=0`` (#1095). A header
longer than the 4 KB detection sample was scored as a fragment, read as one
column, and failed ``require_columns`` (#1098); detection now reads to the end
of the header, up to 1 MiB, and refuses a longer one.

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
    "semicolon rows that comma reads only after cutting": (
        b"id,part;value\nx;y,z,\nu;v,w,\n",
        [("id,part", 0), ("value", 0)],
        0,
    ),
    "BOM, then a blank line": (
        b"\xef\xbb\xbf\nid;value\n1;2\n3;\n",
        [("id", 0), ("value", 1)],
        0,
    ),
    "semicolon rows with a quoted empty trailing field": (
        b'id;amount,EUR\n1;10,50;""\n2;20;""\n',
        [("id", 0), ("amount,EUR", 0)],
        0,
    ),
    "header with a quote inside a field": (
        b'id"x;value\n1;2\n3;4\n',
        [('id"x', 0), ("value", 0)],
        0,
    ),
    "quoted header ending past 4 KB": (
        ('"' + "x" * 4094 + '";value\n1;10\n2;20\n').encode(),
        [("x" * 4094, 0), ("value", 0)],
        0,
    ),
    "BOM, blank line, header past 8 KB": (
        ("﻿\n" + "a" * 10_000 + ";value\n1;2\n3;\n").encode(),
        [("a" * 10_000, 0), ("value", 1)],
        0,
    ),
}

#: Synchronous bytes refuse a ragged row rather than cut it, on every release.
STRICT_BYTES = {
    "semicolon rows with a trailing empty field",
    "semicolon rows with a quoted empty trailing field",
}


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


def test_long_header_gate_sees_every_column(tmp_path: Path) -> None:
    path = tmp_path / "probe.csv"
    path.write_bytes(('"' + "x" * 4094 + '";value\n1;10\n2;20\n').encode())
    assert dp.profile(path).check(require_columns=["value"]).verdict == "pass"


def test_header_past_the_bound_is_refused_not_guessed(tmp_path: Path) -> None:
    data = ("h" * (2**20 + 10) + ";value\n1;2\n").encode()
    path = tmp_path / "probe.csv"
    path.write_bytes(data)
    routes = {
        "bytes": lambda: dp.profile(data, format="csv"),
        "async bytes": lambda: asyncio.run(profile_bytes(data, format="csv")),
        "async file": lambda: asyncio.run(profile_file(path)),
        **{
            engine: (lambda e=engine: dp.profile(path, engine=e))
            for engine in ("auto", "incremental", "columnar")
        },
    }
    wrong = {}
    for route, run in routes.items():
        try:
            run()
        except ValueError as error:
            if "csv_delimiter" not in str(error):
                wrong[route] = repr(error)
        except Exception as error:  # noqa: BLE001 - report every route's outcome at once
            wrong[route] = repr(error)
        else:
            wrong[route] = "read without an error"
    assert not wrong, wrong
    assert len(dp.profile(path, csv_delimiter=";")) == 2
