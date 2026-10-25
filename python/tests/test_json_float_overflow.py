"""A numeric token that overflows a float is a parse error on every route (#1086).

Standard ``json.loads`` turns a finite token like ``1e400`` into ``inf`` without
raising, allowing a payload with overflowed numbers to profile as clean floats
and pass a quality gate. The file and async readers refuse it with "number out
of range". The synchronous bytes route now refuses it too.
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


@pytest.mark.parametrize("overflow_token", ["1e400", "-1e400", "1e1000", "-1e1000"])
def test_json_bytes_float_overflow_refused_under_strict(
    tmp_path: Path, overflow_token: str
) -> None:
    """A float overflow in a JSON document is refused on bytes and file under strict (#1086)."""
    payload = f'[{{"v":{overflow_token}}}]'.encode()
    path = tmp_path / "probe.json"
    path.write_bytes(payload)

    outcomes = _routes(path, payload, "json", "strict")
    for route, outcome in outcomes.items():
        assert isinstance(outcome, str), f"{route} did not raise: {outcome}"
        assert "number out of range" in outcome.lower() or "malformed" in outcome.lower()


@pytest.mark.parametrize("overflow_token", ["1e400", "-1e400"])
def test_jsonl_bytes_float_overflow_parity(tmp_path: Path, overflow_token: str) -> None:
    """JSONL bytes and file agree on float overflow under both strict and skip (#1086)."""
    payload = f'{{"v":{overflow_token}}}\n{{"v":1.5}}\n'.encode()
    path = tmp_path / "probe.jsonl"
    path.write_bytes(payload)

    # Under strict, all routes must refuse
    strict_outcomes = _routes(path, payload, "jsonl", "strict")
    for route, outcome in strict_outcomes.items():
        assert isinstance(outcome, str), f"{route} strict did not raise: {outcome}"

    # Under skip, all routes must drop the overflowed record and profile 1 valid row
    skip_outcomes = _routes(path, payload, "jsonl", "skip")
    assert skip_outcomes == dict.fromkeys(skip_outcomes, (1, 1)), skip_outcomes


def test_in_range_floats_stay_accepted(tmp_path: Path) -> None:
    """In-range floats including extreme exponents and subnormals are accepted."""
    payload = b'[{"v":1.5},{"v":-1e200},{"v":1e308},{"v":0.0},{"v":1e-400}]'
    path = tmp_path / "probe.json"
    path.write_bytes(payload)

    outcomes = _routes(path, payload, "json", "strict")
    assert outcomes == dict.fromkeys(outcomes, (5, 0)), outcomes
