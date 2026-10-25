"""A JSON integer outside the 64-bit range is refused, not rounded.

The file and async readers held such an integer as the nearest float, so
``18446744073709551616`` and ``18446744073709551617`` became one value and
``max_duplicate_rows=0`` failed on a duplicate the input does not contain,
while synchronous bytes kept them apart (#1103). Every route now refuses the
integer as a decode error: strict raises, skip drops and counts the record.
Synchronous JSON bytes load the whole document at once, so they refuse the
document, as they do for a non-standard constant like ``NaN``.

``tests/json_wide_integers.rs`` is the Rust twin.
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

WIDE = "outside the 64-bit range"

#: label -> (format, bytes); the middle record holds the wide integer, so skip
#: must keep reading past it.
CASES: dict[str, tuple[str, bytes]] = {
    "array, above u64": ("json", b'[{"id":1,"v":5},{"id":2,"v":18446744073709551616},{"id":3,"v":6}]'),
    "array, below i64": ("json", b'[{"id":1,"v":5},{"id":2,"v":-9223372036854775809},{"id":3,"v":6}]'),
    "array, nested": ("json", b'[{"id":1,"v":5},{"id":2,"v":{"k":[18446744073709551617]}},{"id":3,"v":6}]'),
    "jsonl": ("jsonl", b'{"id":1,"v":5}\n{"id":2,"v":18446744073709551616}\n{"id":3,"v":6}\n'),
}


def _routes(path: Path, data: bytes, fmt: str, on_error: str) -> dict[str, object]:
    """Each route's (rows, error_count), or the error message it raised."""

    def attempt(call):
        try:
            report = call()
        except ValueError as exc:
            return str(exc)
        return (report.rows, report.to_dict()["execution"]["error_count"])

    return {
        "file": attempt(lambda: dp.profile(path, jsonl_on_error=on_error)),
        "bytes": attempt(lambda: dp.profile(data, format=fmt, jsonl_on_error=on_error)),
        "async file": attempt(lambda: asyncio.run(profile_file(path, jsonl_on_error=on_error))),
        "async bytes": attempt(
            lambda: asyncio.run(profile_bytes(data, format=fmt, jsonl_on_error=on_error))
        ),
    }


def test_wide_integers_are_refused_on_every_route(tmp_path: Path) -> None:
    wrong = {}
    for label, (fmt, data) in CASES.items():
        path = tmp_path / f"probe.{fmt}"
        path.write_bytes(data)

        for route, outcome in _routes(path, data, fmt, "strict").items():
            if not (isinstance(outcome, str) and WIDE in outcome):
                wrong[f"[{label}] strict {route}"] = outcome

        for route, outcome in _routes(path, data, fmt, "skip").items():
            whole_document = route == "bytes" and fmt == "json"
            ok = (
                WIDE in outcome
                if whole_document and isinstance(outcome, str)
                else outcome == (2, 1)
            )
            if not ok:
                wrong[f"[{label}] skip {route}"] = outcome
    assert not wrong, wrong


def test_distinct_wide_keys_never_report_a_duplicate(tmp_path: Path) -> None:
    data = b'[{"v":18446744073709551616},{"v":18446744073709551617}]'
    path = tmp_path / "probe.json"
    path.write_bytes(data)
    for route, outcome in _routes(path, data, "json", "skip").items():
        assert isinstance(outcome, str), (route, outcome)


def test_in_range_integers_floats_and_strings_stay_accepted(tmp_path: Path) -> None:
    data = (
        b'[{"v":18446744073709551615,"s":"18446744073709551616"},'
        b'{"v":-9223372036854775808,"s":"a"},{"v":1e19,"s":"b"},'
        b'{"v":1.8446744073709552e19,"s":"c"}]'
    )
    path = tmp_path / "probe.json"
    path.write_bytes(data)
    outcomes = _routes(path, data, "json", "strict")
    assert outcomes == dict.fromkeys(outcomes, (4, 0)), outcomes
