"""A JSON number that overflows a float is refused, not read as infinity.

``json.loads`` decodes a finite token past the float range, like ``1e400``, as
``inf``. Synchronous bytes then profiled it as one invalid float with no parse
error, and a strict run passed a 100-point gate, while the file and async
readers refused the same bytes as out of range (#1086). Bytes now refuse it as
a decode error: strict raises, skip drops and counts a JSONL record, and a JSON
document, which bytes load at once, is refused whole, as for a wide integer.
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

OVERFLOW = "outside the 64-bit float range"

#: label -> (format, bytes); the middle record holds the overflowing number,
#: so skip must keep reading past it.
CASES: dict[str, tuple[str, bytes]] = {
    "array, exponent": ("json", b'[{"id":1,"v":5},{"id":2,"v":1e400},{"id":3,"v":6}]'),
    "array, negative": ("json", b'[{"id":1,"v":5},{"id":2,"v":-1E400},{"id":3,"v":6}]'),
    "array, nested": ("json", b'[{"id":1,"v":5},{"id":2,"v":{"k":[1e400]}},{"id":3,"v":6}]'),
    "jsonl, exponent": ("jsonl", b'{"id":1,"v":5}\n{"id":2,"v":1e400}\n{"id":3,"v":6}\n'),
    # Just past the largest float: the nearest-float rounding still overflows.
    "jsonl, boundary": (
        "jsonl",
        b'{"id":1,"v":5}\n{"id":2,"v":1.7976931348623159e308}\n{"id":3,"v":6}\n',
    ),
    "jsonl, 400 digits": (
        "jsonl",
        b'{"id":1,"v":5}\n{"id":2,"v":' + b"9" * 400 + b'.5}\n{"id":3,"v":6}\n',
    ),
    # The text decides: a later duplicate key does not hide the literal.
    "jsonl, duplicate key overwrites it": (
        "jsonl",
        b'{"id":1,"v":5}\n{"id":2,"v":1e400,"v":1}\n{"id":3,"v":6}\n',
    ),
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


def test_an_overflowing_number_is_refused_on_every_route(tmp_path: Path) -> None:
    wrong = {}
    for label, (fmt, data) in CASES.items():
        path = tmp_path / f"probe.{fmt}"
        path.write_bytes(data)

        strict = _routes(path, data, fmt, "strict")
        for route, outcome in strict.items():
            if not isinstance(outcome, str):
                wrong[f"[{label}] strict {route}"] = outcome
        if OVERFLOW not in str(strict["bytes"]):
            wrong[f"[{label}] strict bytes message"] = strict["bytes"]

        skip = _routes(path, data, fmt, "skip")
        if fmt == "json":
            # Bytes load the whole document, so they refuse it. The other
            # routes stop at the element and leave the rest unread, as at any
            # decoder error inside an array (#1106): one row, one error.
            if not (isinstance(skip["bytes"], str) and OVERFLOW in skip["bytes"]):
                wrong[f"[{label}] skip bytes"] = skip["bytes"]
            for route in ("file", "async file", "async bytes"):
                if skip[route] != (1, 1):
                    wrong[f"[{label}] skip {route}"] = skip[route]
        else:
            for route, outcome in skip.items():
                if outcome != (2, 1):
                    wrong[f"[{label}] skip {route}"] = outcome
    assert not wrong, wrong


def test_a_strict_gate_cannot_pass_an_overflowing_number() -> None:
    # The issue's example: one invalid float, no error, and a passing gate.
    with pytest.raises(ValueError, match=OVERFLOW):
        dp.profile(b'[{"v":1e400}]', format="json", jsonl_on_error="strict")


def test_floats_in_range_and_underflow_stay_accepted(tmp_path: Path) -> None:
    # The largest float, a literal that rounds down to it, and a literal that
    # underflows to zero are all finite, on every route alike.
    data = (
        b'[{"v":1.7976931348623157e308},{"v":1.7976931348623158e308},'
        b'{"v":-1e308},{"v":1e-400},{"v":2.5}]'
    )
    path = tmp_path / "probe.json"
    path.write_bytes(data)
    outcomes = _routes(path, data, "json", "strict")
    assert outcomes == dict.fromkeys(outcomes, (5, 0)), outcomes
