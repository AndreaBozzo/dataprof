"""A JSON float literal with more digits than a float holds profiles as its digits.

``1000000000000000.01`` reads as ``1000000000000000.0``, so three distinct
amounts became one value with zero variance, while a CSV of the same digits
reported three values and their spread (#1156). A field whose literal does not
survive the round trip now profiles as its text on every route; a literal the
float holds exactly, like ``1.50``, stays a number.

``tests/json_float_digits.rs`` is the Rust twin.
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

#: label -> the ``v`` literals in row order
CASES: dict[str, list[str]] = {
    "more digits than a float": [
        "1000000000000000.01",
        "1000000000000000.05",
        "1000000000000000.02",
    ],
    "below the float range": ["1e-400", "2e-400", "3e-400"],
    "beside held literals": ["0.1", "1000000000000000.01", "1.50", "1e2", "-0.0"],
    "large integer-valued float": ["12345678901234567890.0", "12345678901234567891.0", "7"],
}


def _column(report) -> dict:
    """Column ``v`` without its patterns: a held literal is written in its
    shortest form, so ``1e2`` reads as ``100.0`` and not as scientific."""
    column = next(c for c in report.to_dict()["columns"] if c["name"] == "v")
    column.pop("patterns", None)
    return column


def _routes(tmp_path: Path, fmt: str, data: bytes) -> dict[str, dict]:
    path = tmp_path / f"probe.{fmt}"
    path.write_bytes(data)
    return {
        "file": _column(dp.profile(path)),
        "bytes": _column(dp.profile(data, format=fmt)),
        "async file": _column(asyncio.run(profile_file(path))),
        "async bytes": _column(asyncio.run(profile_bytes(data, format=fmt))),
    }


def test_every_route_profiles_float_digits_as_csv_does(tmp_path: Path) -> None:
    wrong = {}
    for label, values in CASES.items():
        csv = tmp_path / "probe.csv"
        csv.write_text("id,v\n" + "".join(f"{i},{v}\n" for i, v in enumerate(values, 1)))
        expected = _column(dp.profile(csv))

        records = [f'{{"id":{i},"v":{v}}}' for i, v in enumerate(values, 1)]
        documents = {
            "json": ("[" + ",".join(records) + "]").encode(),
            "jsonl": ("\n".join(records) + "\n").encode(),
        }
        for fmt, data in documents.items():
            for route, actual in _routes(tmp_path, fmt, data).items():
                if actual != expected:
                    wrong[f"[{label}] {fmt} {route}"] = (
                        actual["unique_count"],
                        actual.get("stats", {}).get("variance"),
                    )
    assert not wrong, wrong


def test_nested_floats_profile_alike_on_every_route(tmp_path: Path) -> None:
    # Inside a container the float is kept, as the container's JSON text.
    data = b'[{"v":[1000000000000000.01]},{"v":[1000000000000000.05]}]'
    columns = _routes(tmp_path, "json", data)
    assert all(column == columns["file"] for column in columns.values()), columns
