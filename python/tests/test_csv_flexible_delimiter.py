"""``csv_flexible`` sets parsing strictness and nothing else.

With no explicit delimiter, a file profile still detects it. Setting the flag to
either value used to skip detection on every synchronous file engine, so a
semicolon file was split on commas into one column, the null in it was never
counted, and ``max_null_percentage=0`` passed (#1091).

``tests/csv_flexible_delimiter.rs`` is the Rust twin.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

try:
    import dataprof as dp
    from dataprof.asyncio import profile_file
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

ENGINES = ("auto", "incremental", "columnar")
FLAGS = (None, True, False)


def _shape(report: dp.ProfileReport) -> tuple[list[tuple[str, int]], str]:
    columns = [(c["name"], c["null_count"]) for c in report.to_dict()["columns"]]
    return columns, report.check(max_null_percentage=0).verdict


def test_csv_flexible_keeps_delimiter_detection(tmp_path: Path) -> None:
    path = tmp_path / "nulls.csv"
    path.write_text("id;value\n1;10\n2;\n")

    routes = {}
    for engine in ENGINES:
        for flag in FLAGS:
            report = dp.profile(path, engine=engine, csv_flexible=flag)
            routes[f"{engine}, {flag}"] = _shape(report)
    for flag in (True, False):
        routes[f"async, {flag}"] = _shape(asyncio.run(profile_file(path, csv_flexible=flag)))

    expected = ([("id", 0), ("value", 1)], "fail")
    wrong = {route: shape for route, shape in routes.items() if shape != expected}
    assert not wrong, wrong
