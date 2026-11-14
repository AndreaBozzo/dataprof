"""The metric packs a run computed, recorded in ``analysis_settings`` (#1030).

Deselecting a pack moves a score on unchanged data: without ``patterns`` no
column has detected patterns, so validity is not assessed and the overall
score averages the other dimensions. ``analysis_settings["metric_packs"]``
now records the packs, ``compare()`` lists them under ``configuration``, and
a baseline ``check()`` names a ``patterns`` difference as the cause of a
validity or overall change.

``tests/metric_packs_settings.rs`` is the Rust twin. Here the routes are the
Python ones: every file engine, bytes, dicts, pandas, polars and Arrow
inputs, the async functions and SQLite, each report also rebuilt from its
``to_dict()`` summary and saved and loaded.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest

try:
    import dataprof as dp
    from dataprof.asyncio import (
        _HAS_ASYNC,
        profile_bytes as profile_bytes_async,
        profile_file as profile_file_async,
    )
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

# Six of seven emails are valid, so validity is assessed when patterns run.
EMAILS = [
    "ada@example.com",
    "bob@example.com",
    "cy@example.com",
    "di@example.com",
    "ed@example.com",
    "fay@example.com",
    "not an email",
]
COLUMNS = {"id": list(range(1, len(EMAILS) + 1)), "email": EMAILS}
CSV = "id,email\n" + "".join(f"{i},{e}\n" for i, e in zip(COLUMNS["id"], EMAILS, strict=True))
EVERY = ["schema", "statistics", "patterns", "quality"]

# (label, profile() kwargs, the packs the report must record)
SELECTIONS: list[tuple[str, dict[str, Any], list[str]]] = [
    ("default", {}, EVERY),
    ("quality only", {"metrics": ["quality"]}, ["schema", "quality"]),
    (
        "repeated, out of order",
        {"metrics": ["patterns", "statistics", "patterns"]},
        ["schema", "statistics", "patterns"],
    ),
    ("none", {"metrics": []}, ["schema"]),
    ("no quality dimensions", {"quality_dimensions": []}, ["schema", "statistics", "patterns"]),
]


def _routes(tmp_path: Path) -> dict[str, Callable[..., Any]]:
    csv = tmp_path / "t.csv"
    csv.write_text(CSV)
    records = [{"id": i, "email": e} for i, e in zip(COLUMNS["id"], EMAILS, strict=True)]
    as_json = tmp_path / "t.json"
    as_json.write_text(json.dumps(records))
    jsonl = tmp_path / "t.jsonl"
    jsonl.write_text("".join(json.dumps(r) + "\n" for r in records))

    routes: dict[str, Callable[..., Any]] = {
        "csv auto": lambda **k: dp.profile(csv, **k),
        "csv incremental": lambda **k: dp.profile(csv, engine="incremental", **k),
        "csv columnar": lambda **k: dp.profile(csv, engine="columnar", **k),
        "json": lambda **k: dp.profile(as_json, **k),
        "jsonl": lambda **k: dp.profile(jsonl, **k),
        "dict": lambda **k: dp.profile(COLUMNS, **k),
        "records": lambda **k: dp.profile(records, **k),
        "csv bytes": lambda **k: dp.profile(CSV.encode(), format="csv", **k),
    }
    try:
        import pandas as pd
    except ImportError:
        pass
    else:
        routes["pandas"] = lambda **k: dp.profile(pd.DataFrame(COLUMNS), **k)
    try:
        import polars as pl
    except ImportError:
        pass
    else:
        routes["polars"] = lambda **k: dp.profile(pl.DataFrame(COLUMNS), **k)
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        pass
    else:
        parquet = tmp_path / "t.parquet"
        pq.write_table(pa.table(COLUMNS), parquet)
        routes["parquet"] = lambda **k: dp.profile(parquet, **k)
        routes["arrow"] = lambda **k: dp.profile(pa.table(COLUMNS), **k)
    if _HAS_ASYNC:
        routes["async bytes"] = lambda **k: asyncio.run(
            profile_bytes_async(CSV.encode(), format="csv", **k)
        )
        routes["async file"] = lambda **k: asyncio.run(profile_file_async(csv, **k))
    return routes


def _forms(report: dp.ProfileReport, tmp_path: Path) -> list[tuple[str, dp.ProfileReport]]:
    path = tmp_path / "saved.json"
    report.save(str(path))
    return [
        ("report", report),
        ("summary", dp.ProfileReport.from_dict(report.to_dict())),
        ("saved", dp.ProfileReport.load(str(path))),
    ]


def _packs(report: dp.ProfileReport) -> Any:
    settings = report.analysis_settings
    return None if settings is None else settings.get("metric_packs", "absent")


def test_every_route_records_the_packs_it_computed(tmp_path: Path) -> None:
    wrong = []
    for label, kwargs, want in SELECTIONS:
        for route, run in _routes(tmp_path).items():
            for form, report in _forms(run(**kwargs), tmp_path):
                if _packs(report) != want:
                    wrong.append(f"[{label}] {route} {form}: {_packs(report)!r} != {want!r}")
    assert not wrong, "\n".join(wrong)


def test_the_summary_writes_the_packs(tmp_path: Path) -> None:
    report = dp.profile(COLUMNS, metrics=["quality"])
    assert report.to_dict()["analysis_settings"]["metric_packs"] == ["schema", "quality"]


def test_a_database_query_records_its_packs(tmp_path: Path) -> None:
    from dataprof import _dataprof

    if not hasattr(_dataprof, "analyze_database_async"):
        pytest.skip("database features not compiled")
    path = tmp_path / "t.db"
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute("CREATE TABLE t (id INTEGER, email TEXT)")
        connection.executemany("INSERT INTO t VALUES (?, ?)", list(zip(*COLUMNS.values())))

    def run(**kwargs: Any) -> Any:
        return asyncio.run(dp.analyze_database_async(str(path), "SELECT * FROM t", **kwargs))

    cases = [
        ("default", run(), EVERY),
        ("quality only", run(config=dp.ProfilerConfig(metrics=["quality"])), ["schema", "quality"]),
        ("calculate_quality=False", run(calculate_quality=False), EVERY[:3]),
    ]
    wrong = [f"{label}: {_packs(r)!r}" for label, r, want in cases if _packs(r) != want]
    assert not wrong, "\n".join(wrong)


def _legacy(report: dp.ProfileReport) -> dict[str, Any]:
    """The summary as a release before 0.14 wrote it: no ``metric_packs``."""
    summary = report.to_dict()
    del summary["analysis_settings"]["metric_packs"]
    return summary


def test_a_summary_without_packs_loads_them_as_unrecorded() -> None:
    loaded = dp.ProfileReport.from_dict(_legacy(dp.profile(COLUMNS)))
    assert loaded.analysis_settings is not None
    assert "metric_packs" not in loaded.analysis_settings
    assert "metric_packs" not in loaded.to_dict()["analysis_settings"]


@pytest.mark.parametrize("packs", [None, ["schema", "lineage"], "schema"])
def test_a_null_or_unknown_pack_list_is_malformed(packs: Any) -> None:
    summary = dp.profile(COLUMNS).to_dict()
    summary["analysis_settings"]["metric_packs"] = packs
    with pytest.raises(ValueError, match="metric_packs"):
        dp.ProfileReport.from_dict(summary)


def _sides(report: dp.ProfileReport) -> list[tuple[str, dp.ProfileReport]]:
    return [("report", report), ("summary", dp.ProfileReport.from_dict(report.to_dict()))]


def test_compare_lists_each_sides_packs() -> None:
    every = dp.profile(COLUMNS)
    quality = dp.profile(COLUMNS, metrics=["quality"])
    reordered = dp.profile(COLUMNS, metrics=["quality", "patterns", "statistics"])
    legacy = dp.ProfileReport.from_dict(_legacy(every))
    cases = [
        ("differ", every, quality, {"a": EVERY, "b": ["schema", "quality"], "same": False}),
        ("same packs, other order", every, reordered, {"a": EVERY, "b": EVERY, "same": True}),
        ("baseline before 0.14", legacy, every, {"a": None, "b": EVERY, "same": None}),
    ]
    wrong = []
    for label, a, b, want in cases:
        for name_a, side_a in _sides(a):
            for name_b, side_b in _sides(b):
                got = side_a.compare(side_b)["configuration"]["metric_packs"]
                if got != want:
                    wrong.append(f"[{label}] {name_a}/{name_b}: {got!r} != {want!r}")
    assert not wrong, "\n".join(wrong)


CHANGED = "not_evaluated setting_changed metric_packs"


def _gate(report: dp.ProfileReport, baseline: dp.ProfileReport) -> list[str]:
    """Each score rule's outcome, overall first, then validity."""
    result = report.check(
        baseline=baseline,
        max_quality_score_drop=0.0,
        max_dimension_score_drops={"validity": 0.0},
    ).to_dict()
    return [
        " ".join(str(check[key]) for key in ("status", "reason", "setting") if check.get(key))
        for check in result["checks"]
    ]


def test_the_gate_names_a_patterns_difference_as_the_cause() -> None:
    every = dp.profile(COLUMNS)
    no_patterns = dp.profile(COLUMNS, metrics=["statistics", "quality"])
    no_statistics = dp.profile(COLUMNS, metrics=["patterns", "quality"])
    legacy = dp.ProfileReport.from_dict(_legacy(every))
    cases = [
        ("patterns dropped", no_patterns, every, [CHANGED, CHANGED]),
        ("patterns added", every, no_patterns, [CHANGED, CHANGED]),
        ("statistics dropped", no_statistics, every, ["passed", "passed"]),
        (
            "baseline before 0.14",
            no_patterns,
            legacy,
            ["not_evaluated dimensions_changed", "not_evaluated not_assessed"],
        ),
        ("baseline before 0.14, same packs", every, legacy, ["passed", "passed"]),
    ]
    wrong = []
    for label, report, baseline, want in cases:
        for name, side in _sides(baseline):
            got = _gate(report, side)
            if got != want:
                wrong.append(f"[{label}] baseline {name}: {got!r} != {want!r}")
    assert not wrong, "\n".join(wrong)


def test_packs_that_differ_without_moving_validity_leave_the_overall_rule_decided() -> None:
    # No column holds a pattern, so validity is assessed on neither side.
    columns = {"id": [1, 2, 3], "amount": [10, 20, 30]}
    every = dp.profile(columns)
    no_patterns = dp.profile(columns, metrics=["statistics", "quality"])
    assert _gate(no_patterns, every) == ["passed", CHANGED]
