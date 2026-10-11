"""Row-only stop trees take effect on all sources. Rust twin: tests/memory_row_limits.rs."""

from pathlib import Path

import dataprof as dp
from _profile_routes import _routes


def test_row_limit_trees_apply_to_every_input_route(tmp_path: Path):
    sc = dp.StopCondition
    cases = [
        ("leaf", sc.max_rows(3), 3),
        ("any", sc.max_rows(3) | sc.max_rows(7), 3),
        ("all", sc.max_rows(3) & sc.max_rows(7), 7),
        ("nested", (sc.max_rows(3) | sc.max_rows(7)) & sc.max_rows(5), 5),
        ("never", sc.never(), 20),
    ]
    wrong = []
    for case, stop, expected in cases:
        for label, run in _routes(tmp_path, stop_condition=stop).items():
            report = run(case, [str(i) for i in range(20)])
            if report.rows != expected:
                wrong.append(f"[{label}] {case}: {report.rows}, expected {expected}")
    assert not wrong, "\n".join(wrong)


def test_every_in_memory_route_rejects_unsupported_controls(tmp_path: Path):
    cases = [
        ("schema", {"stop_condition": dp.StopCondition.schema_stable(1)}, "only row-limit"),
        ("sample", {"sampling": dp.SamplingStrategy.random(1)}, "sampling is not applied"),
        (
            "conflict",
            {"max_rows": 1, "stop_condition": dp.StopCondition.max_rows(1)},
            "both max_rows and stop_condition",
        ),
    ]
    memory_routes = {"dict", "rows", "arrow", "pandas", "polars"}
    wrong = []
    for case, options, expected in cases:
        for label, run in _routes(tmp_path, **options).items():
            if label not in memory_routes:
                continue
            try:
                run(case, ["1", "2"])
            except Exception as error:
                if not isinstance(error, ValueError) or expected not in str(error):
                    wrong.append(f"[{label}] {case}: {type(error).__name__}: {error}")
            else:
                wrong.append(f"[{label}] {case}: unsupported control accepted")
    for label, run in _routes(tmp_path, sampling=dp.SamplingStrategy.none()).items():
        if label in memory_routes and run("noop", ["1", "2"]).rows != 2:
            wrong.append(f"[{label}] noop sampling changed the population")
    assert not wrong, "\n".join(wrong)
