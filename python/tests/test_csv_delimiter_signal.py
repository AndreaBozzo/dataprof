"""A CSV report records the delimiter and what detection saw (#1100).

``findings()`` warns with ``delimiter_ambiguous`` when a second candidate reads
the file as well as the chosen one, and with ``header_not_split`` when no
candidate splits the header while a character outside them splits the header
and every record alike. A plain one-column file and a configured delimiter get
no finding. Detection itself does not change.

``tests/csv_delimiter_signal.rs`` is the Rust twin.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
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

#: ``(case, source, detection recorded, finding codes)``.
CASES: list[tuple[str, bytes, dict[str, Any], list[str]]] = [
    (
        "comma and semicolon tie",
        b"id;amount,EUR\n1;10,50;\n2;20,75;\n",
        {"delimiter": ",", "header_fields": 2, "runner_up": ";"},
        ["delimiter_ambiguous"],
    ),
    (
        "the mirror tie",
        b"id,part;value\nx;y,z,\nu;v,w,\n",
        {"delimiter": ";", "header_fields": 2, "runner_up": ","},
        ["delimiter_ambiguous"],
    ),
    (
        "delimiter outside the candidates",
        b"id^amount\n1^10\n2^20\n",
        {"delimiter": ",", "header_fields": 1, "unlisted_delimiter": "^"},
        ["header_not_split"],
    ),
    ("one-column file", b"value\n1\n2\n3\n", {"delimiter": ",", "header_fields": 1}, []),
    ("clear comma", b"a,b,c\n1,2,3\n4,5,6\n", {"delimiter": ",", "header_fields": 3}, []),
]

DELIMITER_CODES = {"delimiter_ambiguous", "header_not_split"}


def _routes(path: Path, data: bytes, **kwargs: Any) -> dict[str, Callable[[], dp.ProfileReport]]:
    routes: dict[str, Callable[[], dp.ProfileReport]] = {
        engine: (lambda engine=engine: dp.profile(path, engine=engine, **kwargs))
        for engine in ("auto", "incremental", "columnar")
    }
    routes["bytes"] = lambda: dp.profile(data, format="csv", **kwargs)
    if dp.capabilities().async_streaming:
        routes["async file"] = lambda: asyncio.run(profile_file(path, **kwargs))
        routes["async bytes"] = lambda: asyncio.run(profile_bytes(data, format="csv", **kwargs))
    return routes


def _observed(report: dp.ProfileReport) -> tuple[Any, list[str]]:
    metadata = json.loads(report.to_json())["data_source"].get("csv_metadata")
    codes = [finding.code for finding in report.findings() if finding.code in DELIMITER_CODES]
    return metadata, codes


def test_every_route_records_what_delimiter_detection_saw(tmp_path: Path) -> None:
    wrong = []
    for case, data, detection, codes in CASES:
        path = tmp_path / "source.csv"
        path.write_bytes(data)
        wanted = ({"delimiter": detection["delimiter"], "delimiter_detection": detection}, codes)
        for route, run in _routes(path, data).items():
            seen = _observed(run())
            if seen != wanted:
                wrong.append(f"{route}, {case}: got {seen}, want {wanted}")
    assert not wrong, "\n".join(wrong)


def test_a_configured_delimiter_records_no_detection_and_no_finding(tmp_path: Path) -> None:
    # The clear file: synchronous bytes are always strict, and the tie file
    # read on semicolon leaves a trailing empty field the header lacks.
    data = CASES[-1][1]
    path = tmp_path / "source.csv"
    path.write_bytes(data)
    wrong = []
    for route, run in _routes(path, data, csv_delimiter=",").items():
        seen = _observed(run())
        if seen != ({"delimiter": ","}, []):
            wrong.append(f"{route}: {seen}")
    assert not wrong, "\n".join(wrong)


def test_a_stored_report_keeps_the_detection_and_the_finding(tmp_path: Path) -> None:
    path = tmp_path / "source.csv"
    path.write_bytes(CASES[0][1])
    report = dp.profile(path)
    wanted = _observed(report)
    wrong = []
    for dialect, stored in (
        ("to_dict", dp.ProfileReport.from_dict(report.to_dict())),
        ("to_json", dp.ProfileReport.from_json(report.to_json())),
    ):
        codes = [f.code for f in stored.findings() if f.code in DELIMITER_CODES]
        if (stored.csv_metadata, codes) != (report.csv_metadata, wanted[1]):
            wrong.append(f"{dialect}: {stored.csv_metadata}, {codes}")
    assert not wrong, "\n".join(wrong)
