"""Schema-stability composition; Rust twin: tests/stop_condition_composition.rs."""

from pathlib import Path

import dataprof as dp


def test_schema_stability_is_evaluated_inside_each_composite(tmp_path: Path):
    path = tmp_path / "stable.csv"
    path.write_bytes(b"value\n" + b"12345\n" * 20_000)
    sc = dp.StopCondition
    cases = [
        ("all_row_and_schema", sc.max_rows(15_000) & sc.schema_stable(100), 15_000, 15_200),
        (
            "all_two_schema_thresholds",
            sc.schema_stable(100) & sc.schema_stable(3_000),
            3_000,
            3_400,
        ),
        (
            "nested",
            (sc.schema_stable(100) | sc.max_rows(1_000)) & sc.max_rows(15_000),
            15_000,
            15_200,
        ),
        ("needs_baseline", sc.schema_stable(1), 190, 400),
    ]
    wrong = []
    for case, condition, lower, upper in cases:
        report = dp.profile(path, engine="incremental", chunk_size=600, stop_condition=condition)
        if not lower <= report.rows <= upper:
            wrong.append(f"[incremental] {case}: {report.rows}, expected {lower}..={upper}")
        if report.source_exhausted or report.truncation_reason is None:
            wrong.append(f"[incremental] {case}: missing truncation provenance")
    assert not wrong, "\n".join(wrong)
