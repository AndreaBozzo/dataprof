"""A JSON record the async route cannot read is judged only when the cap reaches it (#1206).

The reader stops at the row cap only when the cap counts every row read; under
a systematic sample it parses ahead of the cap, and counted or refused the
records past it there. ``tests/async_json_row_cap.rs`` is the Rust twin.
"""

from __future__ import annotations

import asyncio

import pytest

try:
    import dataprof as dp
    from dataprof.asyncio import profile_bytes
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

if not dp.capabilities().async_streaming:
    pytest.skip("Async streaming not compiled.", allow_module_level=True)

_ROWS = [b'{"a":%d}' % i for i in range(1, 8)]

#: ``(case, format, cap, source, faults the cap reaches)``. A cap of three kept
#: rows stops by the fifth row under every sampler below; content after the
#: closing bracket follows the last row, which a cap of five reaches under each.
CASES = [
    (
        "JSONL, malformed lines past the cap",
        "jsonl",
        3,
        b"\n".join([*_ROWS, b"{bad", b"[1]"]),
        0,
    ),
    (
        "JSONL, malformed lines on both sides of the cap",
        "jsonl",
        3,
        b"\n".join([b"{bad", *_ROWS, b"{bad"]),
        1,
    ),
    (
        "JSON array, non-object elements past the cap",
        "json",
        3,
        b"[" + b",".join([*_ROWS, b"1", b'"x"']) + b"]",
        0,
    ),
    (
        "JSON array, non-object elements on both sides of the cap",
        "json",
        3,
        b"[" + b",".join([b"1", *_ROWS, b'"x"']) + b"]",
        1,
    ),
    (
        "JSON array, content after the closing bracket at the cap",
        "json",
        5,
        b"[" + b",".join(_ROWS[:5]) + b"] trailing",
        1,
    ),
]

SAMPLERS = {
    "no sampling": None,
    "systematic(2)": dp.SamplingStrategy.systematic(2),
    "reservoir(20)": dp.SamplingStrategy.reservoir(20),
}


def _profile(data: bytes, fmt: str, cap: int, sampling, chunk_size: int | None, policy: str):
    return asyncio.run(
        profile_bytes(
            data,
            format=fmt,
            sampling=sampling,
            chunk_size=chunk_size,
            max_rows=cap,
            jsonl_on_error=policy,
        )
    )


def test_tolerant_json_counts_only_the_faults_the_cap_reaches() -> None:
    wrong = []
    for case, fmt, cap, data, reached in CASES:
        for sampler, sampling in SAMPLERS.items():
            for chunk_size in (None, 1):
                route = f"{case}, {sampler}, chunk_size={chunk_size}"
                try:
                    count = _profile(data, fmt, cap, sampling, chunk_size, "skip").error_count
                except Exception as error:  # noqa: BLE001 - every failure is a finding
                    wrong.append(f"{route}: {error}")
                    continue
                if count != reached:
                    wrong.append(f"{route}: error_count {count}, expected {reached}")
    assert not wrong, "\n".join(wrong)


def test_strict_json_refuses_only_a_fault_the_cap_reaches() -> None:
    wrong = []
    for case, fmt, cap, data, reached in CASES:
        for sampler, sampling in SAMPLERS.items():
            for chunk_size in (None, 1):
                route = f"{case}, {sampler}, chunk_size={chunk_size}"
                try:
                    _profile(data, fmt, cap, sampling, chunk_size, "strict")
                    refused = False
                except ValueError:
                    refused = True
                if refused != (reached > 0):
                    wrong.append(f"{route}: refused={refused}, expected {reached > 0}")
    assert not wrong, "\n".join(wrong)
