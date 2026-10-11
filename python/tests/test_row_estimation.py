"""Logical record counts and stopping fractions. Rust twin: tests/row_estimation.rs."""

import asyncio
from pathlib import Path

import dataprof as dp


def fixtures():
    return {
        "growing": ("a,b\n" + "".join(f"{i},{i * 2}\n" for i in range(200_000)), 200_000),
        "quoted": (
            "id,note\n" + "".join(f'{i},"{"x" * 400}\nline two {i}"\n' for i in range(30_000)),
            30_000,
        ),
        "short": ("value\n" + "123456789\n" * 20_000, 20_000),
        "wide": ("value\n" + ("7" * 100 + "\n") * 20_000, 20_000),
    }


def test_quick_count_uses_records_and_reports_exactness(tmp_path: Path):
    wrong = []
    for case, (text, expected) in fixtures().items():
        path = tmp_path / f"{case}.csv"
        path.write_bytes(text.encode())
        count = dp.quick_row_count(path)
        if count.count != expected or not count.exact or count.relative_error is not None:
            wrong.append(f"{case}: {count}, expected exact {expected}")
    assert not wrong, "\n".join(wrong)


def test_confidence_means_fraction_of_logical_records(tmp_path: Path):
    from dataprof.asyncio import profile_bytes

    wrong = []
    for case, (text, total) in fixtures().items():
        path = tmp_path / f"{case}.csv"
        path.write_bytes(text.encode())
        routes = {
            "incremental": dp.profile(
                path,
                engine="incremental",
                chunk_size=4096,
                stop_condition=dp.StopCondition.confidence_threshold(0.5),
            )
        }
        if dp.capabilities().async_streaming:
            routes["async bytes"] = asyncio.run(
                profile_bytes(
                    text.encode(),
                    format="csv",
                    chunk_size=4096,
                    stop_condition=dp.StopCondition.confidence_threshold(0.5),
                )
            )
        for route, report in routes.items():
            if not total // 2 <= report.rows <= total // 2 + 1024:
                wrong.append(f"[{route}] {case}: {report.rows}, expected half of {total}")
    assert not wrong, "\n".join(wrong)
