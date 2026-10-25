"""A JSON array read that stops at a fault does not report the source as read.

Under the skip policy, the array scanner cannot read past an element it failed
to decode, such as ``1e400`` or a syntax error, so it drains the rest. The
records after the fault were never read, yet the report said the source was
exhausted, so a gate saw a partial profile as a complete one (#1106). Anything
left unread after the fault now marks the source as not exhausted, and the
gate names the gap as truncation.

``tests/json_array_fault_drain.rs`` is the Rust twin.
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

#: label -> (file, expected (rows, errors, source exhausted, gate gap))
CASES: dict[str, tuple[bytes, tuple[int, int, bool, str | None]]] = {
    "number out of f64 range mid-array": (
        b'[{"a":1},{"a":1e400},{"a":2},{"a":3}]',
        (1, 1, False, "truncated"),
    ),
    "syntax error mid-array": (b'[{"a":1},{"a":},{"a":2}]', (1, 1, False, "truncated")),
    "input ends before the closing bracket": (
        b'[{"a":1},{"a":2}',
        (2, 1, True, "records_skipped"),
    ),
    "clean array": (b'[{"a":1},{"a":2}]', (2, 0, True, None)),
}


def _shape(report: dp.ProfileReport) -> tuple[int, int, bool, str | None]:
    execution = report.to_dict()["execution"]
    evidence = report.check(min_rows=1).to_dict()["evidence"]
    return (
        report.rows,
        execution["error_count"],
        execution["source_exhausted"],
        evidence.get("reason"),
    )


def test_a_read_stopped_at_a_fault_is_not_exhausted(tmp_path: Path) -> None:
    wrong = {}
    for label, (data, expected) in CASES.items():
        path = tmp_path / "probe.json"
        path.write_bytes(data)
        for route, report in (
            ("file", dp.profile(path)),
            ("async file", asyncio.run(profile_file(path))),
        ):
            shape = _shape(report)
            if shape != expected:
                wrong[f"[{label}] {route}"] = shape
    assert not wrong, wrong
