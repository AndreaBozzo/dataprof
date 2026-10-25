"""The JSON integer ``-0`` profiles as ``0`` on every route.

serde_json reads the integer literal ``-0`` as the float ``-0.0``, while
Python's ``json`` reads it as the integer ``0``. ``[{"v":0},{"v":-0}]`` was
then one integer value and a duplicate row over synchronous bytes, and two
distinct floats over the file and async readers, so ``max_duplicate_rows=0``
passed or failed with the transport. The float literal ``-0.0`` keeps its sign
on every route.
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

#: label -> (first record, second record, duplicate rows on every route)
CASES: dict[str, tuple[str, str, int]] = {
    "integer": ('{"v":0}', '{"v":-0}', 1),
    "beside a string": ('{"v":-0,"x":"a"}', '{"v":0,"x":"a"}', 1),
    "nested in an array": ('{"v":[0,1]}', '{"v":[-0,1]}', 1),
    "nested in an object": ('{"v":{"k":0}}', '{"v":{"k":-0}}', 1),
    "key holding -0 text": ('{"-0":-0}', '{"-0":0}', 1),
    "float control": ('{"v":0.0}', '{"v":-0.0}', 0),
}


def _summary(report) -> tuple:
    doc = report.to_dict()
    return (
        report.rows,
        tuple((c["name"], c["data_type"], c["unique_count"]) for c in doc["columns"]),
        doc["quality"]["uniqueness"]["duplicate_rows"],
        report.check(max_duplicate_rows=0).verdict,
    )


@pytest.mark.parametrize("fmt", ["json", "jsonl"])
@pytest.mark.parametrize("label", list(CASES))
def test_negative_zero_profiles_alike_on_every_route(tmp_path: Path, fmt: str, label: str) -> None:
    first, second, duplicates = CASES[label]
    data = (f"[{first},{second}]" if fmt == "json" else f"{first}\n{second}\n").encode()
    path = tmp_path / f"zero.{fmt}"
    path.write_bytes(data)

    routes = {
        "file": _summary(dp.profile(path)),
        "bytes": _summary(dp.profile(data, format=fmt)),
        "async file": _summary(asyncio.run(profile_file(path))),
        "async bytes": _summary(asyncio.run(profile_bytes(data, format=fmt))),
    }

    assert len(set(routes.values())) == 1, routes
    assert routes["file"][2] == duplicates


@pytest.mark.parametrize("data", [b'{"v":-0}', b'{\n  "v": {"k": -0}\n}'])
def test_negative_zero_in_a_single_object_document(tmp_path: Path, data: bytes) -> None:
    path = tmp_path / "zero.json"
    path.write_bytes(data)
    routes = {
        "file": dp.profile(path),
        "bytes": dp.profile(data, format="json"),
        "async file": asyncio.run(profile_file(path)),
        "async bytes": asyncio.run(profile_bytes(data, format="json")),
    }
    columns = {name: r.to_dict()["columns"] for name, r in routes.items()}
    assert all(c == columns["bytes"] for c in columns.values()), columns
