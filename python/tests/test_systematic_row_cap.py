"""A row cap under sampling analyzes the same rows whatever the chunk size (#1208).

A systematic interval counts the rows it keeps against ``max_rows``. The check
after each chunk counted every row read, so with small chunks systematic(2)
under ``max_rows=5`` stopped after five rows read and three kept, and still
named the cap of five. ``tests/systematic_row_cap.rs`` is the Rust twin.
"""

from __future__ import annotations

import asyncio

import pytest

try:
    import dataprof as dp
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

ROWS = 20

SAMPLERS = {
    "no sampling": None,
    "systematic(2)": dp.SamplingStrategy.systematic(2),
    "reservoir(8)": dp.SamplingStrategy.reservoir(8),
}

#: ``(label, stop condition, the cap it enforces)``. The composite names its
#: larger cap first: when the fifth kept row stops a systematic(2) scan, nine
#: rows have been read, so a cap counted on rows read also meets eight and the
#: report names ``max_rows(8)``, a cap that did not stop the scan.
CONDITIONS = [
    ("max_rows(5)", dp.StopCondition.max_rows(5), 5),
    ("max_rows(10)", dp.StopCondition.max_rows(10), 10),
    ("max_rows(11)", dp.StopCondition.max_rows(11), 11),
    (
        "max_rows(8) | max_rows(5)",
        dp.StopCondition.max_rows(8) | dp.StopCondition.max_rows(5),
        5,
    ),
]


def _csv() -> bytes:
    lines = ["id,value", *(f"r{i},{100 + i}" for i in range(ROWS))]
    return ("\n".join(lines) + "\n").encode()


def _jsonl() -> bytes:
    return "".join(f'{{"id":"r{i}","value":{100 + i}}}\n' for i in range(ROWS)).encode()


def _expected(sampler: str, cap: int) -> tuple[int, str | None]:
    """Rows analyzed and truncation reason for 20 rows under a cap of ``cap``.

    systematic(2) keeps rows 0, 2, 4, ..., so a cap of 10 is reached at row 18
    with row 19 unread and a cap of 11 is never reached. A reservoir caps the
    rows read and samples eight of them.
    """
    if sampler == "systematic(2)" and cap == 11:
        return 10, None
    rows = min(cap, 8) if sampler == "reservoir(8)" else cap
    return rows, f"max_rows({cap})"


def _check(route: str, sampler: str, cap: int, report, wrong: list[str]) -> None:
    expected = _expected(sampler, cap)
    actual = (report.rows, report.truncation_reason)
    if actual != expected:
        wrong.append(f"{route}: {actual}, expected {expected}")


def test_incremental_row_cap_does_not_depend_on_the_chunk_size(tmp_path) -> None:
    path = tmp_path / "rows.csv"
    path.write_bytes(_csv())
    wrong: list[str] = []
    # Chunk sizes are bytes on this engine: 1 reads one record per chunk, 16
    # two or three, and the default the whole file.
    for sampler, sampling in SAMPLERS.items():
        for condition, stop, cap in CONDITIONS:
            for chunk_size in (1, 16, None):
                route = f"incremental, {sampler}, {condition}, chunk_size={chunk_size}"
                report = dp.profile(
                    str(path),
                    engine="incremental",
                    sampling=sampling,
                    stop_condition=stop,
                    chunk_size=chunk_size,
                )
                _check(route, sampler, cap, report, wrong)
    assert not wrong, "\n".join(wrong)


@pytest.mark.skipif(not dp.capabilities().async_streaming, reason="Async streaming not compiled.")
def test_async_row_cap_does_not_depend_on_the_chunk_size() -> None:
    from dataprof.asyncio import profile_bytes

    wrong: list[str] = []
    # Chunk sizes are records on this route.
    for fmt, data in (("csv", _csv()), ("jsonl", _jsonl())):
        for sampler, sampling in SAMPLERS.items():
            for condition, stop, cap in CONDITIONS:
                for chunk_size in (1, 4, None):
                    route = f"async {fmt}, {sampler}, {condition}, chunk_size={chunk_size}"
                    report = asyncio.run(
                        profile_bytes(
                            data,
                            format=fmt,
                            sampling=sampling,
                            stop_condition=stop,
                            chunk_size=chunk_size,
                        )
                    )
                    _check(route, sampler, cap, report, wrong)
    assert not wrong, "\n".join(wrong)
