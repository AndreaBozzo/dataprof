"""Which column pairs with which in ``temporal_pairs``, recorded in
``analysis_settings`` (#1029).

A report recorded the columns ``temporal_pairs`` named, not which column
paired with which. Two runs that pair the same columns differently score
timeliness differently on the same data, so a baseline ``check()`` read any
non-empty ``temporal_pairs`` as ``setting_unrecorded``.
``analysis_settings["temporal_pairs"]`` now records the pairs, ``compare()``
lists them under ``configuration``, and ``check()`` compares them.

``tests/temporal_pairs_record.rs`` is the Rust twin. Here the routes are the
Python ones: every file engine, bytes, dicts, pandas, polars and Arrow
inputs and the async functions, each report also rebuilt from its
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

# Three date columns whose names carry no role word, so only a declared pair
# compares them. `alpha` precedes `beta` on every row; `gamma` falls before
# both on the last row.
COLUMNS = {
    "alpha": ["2024-01-01", "2024-01-05", "2024-01-10"],
    "beta": ["2024-02-01", "2024-02-05", "2024-02-10"],
    "gamma": ["2024-03-01", "2024-03-05", "2024-01-02"],
}
ROWS = [dict(zip(COLUMNS, row, strict=True)) for row in zip(*COLUMNS.values(), strict=True)]
CSV = ",".join(COLUMNS) + "\n" + "".join(",".join(row.values()) + "\n" for row in ROWS)

AB = {"start": "alpha", "end": "beta"}
BA = {"start": "beta", "end": "alpha"}
BG = {"start": "beta", "end": "gamma"}

# (label, the pairs declared, the pairs the report must record)
DECLARATIONS: list[tuple[str, list[tuple[str, str]], list[dict[str, str]]]] = [
    ("none", [], []),
    ("one", [("beta", "alpha")], [BA]),
    (
        "repeated, out of order",
        [("beta", "gamma"), ("alpha", "beta"), ("beta", "gamma")],
        [AB, BG],
    ),
]


def _routes(tmp_path: Path) -> dict[str, Callable[..., Any]]:
    csv = tmp_path / "t.csv"
    csv.write_text(CSV)
    as_json = tmp_path / "t.json"
    as_json.write_text(json.dumps(ROWS))
    jsonl = tmp_path / "t.jsonl"
    jsonl.write_text("".join(json.dumps(r) + "\n" for r in ROWS))

    routes: dict[str, Callable[..., Any]] = {
        "csv auto": lambda **k: dp.profile(csv, **k),
        "csv incremental": lambda **k: dp.profile(csv, engine="incremental", **k),
        "csv columnar": lambda **k: dp.profile(csv, engine="columnar", **k),
        "json": lambda **k: dp.profile(as_json, **k),
        "jsonl": lambda **k: dp.profile(jsonl, **k),
        "dict": lambda **k: dp.profile(COLUMNS, **k),
        "records": lambda **k: dp.profile(ROWS, **k),
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


def _pairs(report: dp.ProfileReport) -> Any:
    settings = report.analysis_settings
    return None if settings is None else settings.get("temporal_pairs", "absent")


def test_every_route_records_the_declared_pairs(tmp_path: Path) -> None:
    wrong = []
    for label, declared, want in DECLARATIONS:
        for route, run in _routes(tmp_path).items():
            for form, report in _forms(run(temporal_pairs=declared), tmp_path):
                if _pairs(report) != want:
                    wrong.append(f"[{label}] {route} {form}: {_pairs(report)!r} != {want!r}")
    assert not wrong, "\n".join(wrong)


def test_the_summary_writes_the_pairs() -> None:
    report = dp.profile(COLUMNS, temporal_pairs=[("beta", "gamma"), ("alpha", "beta")])
    assert report.to_dict()["analysis_settings"]["temporal_pairs"] == [AB, BG]


def test_a_database_query_records_no_pairs(tmp_path: Path) -> None:
    from dataprof import _dataprof

    if not hasattr(_dataprof, "analyze_database_async"):
        pytest.skip("database features not compiled")
    path = tmp_path / "t.db"
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute("CREATE TABLE t (alpha TEXT, beta TEXT, gamma TEXT)")
        connection.executemany(
            "INSERT INTO t VALUES (?, ?, ?)", [tuple(row.values()) for row in ROWS]
        )
    # Database profiling takes no semantic hints yet, so a query records that
    # it declared none: an empty list, never an absent one.
    report = asyncio.run(dp.analyze_database_async(str(path), "SELECT * FROM t"))
    assert _pairs(report) == []


def _legacy(report: dp.ProfileReport) -> dict[str, Any]:
    """The summary as a release before 0.14 wrote it: no ``temporal_pairs``."""
    summary = report.to_dict()
    del summary["analysis_settings"]["temporal_pairs"]
    return summary


def test_a_summary_without_pairs_loads_them_as_unrecorded() -> None:
    report = dp.profile(COLUMNS, temporal_pairs=[("alpha", "beta")])
    loaded = dp.ProfileReport.from_dict(_legacy(report))
    assert loaded.analysis_settings is not None
    assert "temporal_pairs" not in loaded.analysis_settings
    assert "temporal_pairs" not in loaded.to_dict()["analysis_settings"]


@pytest.mark.parametrize(
    "pairs",
    [
        None,
        {"start": "alpha", "end": "beta"},
        [["alpha", "beta"]],
        [{"start": "alpha"}],
        [{"start": "alpha", "end": None}],
    ],
)
def test_a_null_or_malformed_pair_list_is_rejected(pairs: Any) -> None:
    summary = dp.profile(COLUMNS).to_dict()
    summary["analysis_settings"]["temporal_pairs"] = pairs
    with pytest.raises(ValueError, match="temporal_pairs"):
        dp.ProfileReport.from_dict(summary)


def _sides(report: dp.ProfileReport) -> list[tuple[str, dp.ProfileReport]]:
    return [("report", report), ("summary", dp.ProfileReport.from_dict(report.to_dict()))]


def test_compare_lists_each_sides_pairs() -> None:
    forward = dp.profile(COLUMNS, temporal_pairs=[("alpha", "beta")])
    reversed_ = dp.profile(COLUMNS, temporal_pairs=[("beta", "alpha")])
    chained = dp.profile(COLUMNS, temporal_pairs=[("alpha", "beta"), ("beta", "gamma")])
    shuffled = dp.profile(
        COLUMNS, temporal_pairs=[("beta", "gamma"), ("alpha", "beta"), ("alpha", "beta")]
    )
    none = dp.profile(COLUMNS)
    legacy = dp.ProfileReport.from_dict(_legacy(forward))
    cases = [
        ("reversed", forward, reversed_, {"a": [AB], "b": [BA], "same": False}),
        (
            "same pairs, other order and repetition",
            chained,
            shuffled,
            {"a": [AB, BG], "b": [AB, BG], "same": True},
        ),
        ("none declared on one side", none, forward, {"a": [], "b": [AB], "same": False}),
        ("baseline before 0.14", legacy, forward, {"a": None, "b": [AB], "same": None}),
    ]
    wrong = []
    for label, a, b, want in cases:
        for name_a, side_a in _sides(a):
            for name_b, side_b in _sides(b):
                got = side_a.compare(side_b)["configuration"]["temporal_pairs"]
                if got != want:
                    wrong.append(f"[{label}] {name_a}/{name_b}: {got!r} != {want!r}")
    assert not wrong, "\n".join(wrong)


CHANGED = "not_evaluated setting_changed temporal_pairs"
UNRECORDED = "not_evaluated setting_unrecorded temporal_pairs"


def _gate(report: dp.ProfileReport, baseline: dp.ProfileReport) -> list[str]:
    """Each score rule's outcome, overall first, then timeliness."""
    result = report.check(
        baseline=baseline,
        max_quality_score_drop=0.0,
        max_dimension_score_drops={"timeliness": 0.0},
    ).to_dict()
    return [
        " ".join(str(check[key]) for key in ("status", "reason", "setting") if check.get(key))
        for check in result["checks"]
    ]


def test_the_gate_compares_the_recorded_pairs() -> None:
    forward = dp.profile(COLUMNS, temporal_pairs=[("alpha", "beta")])
    reversed_ = dp.profile(COLUMNS, temporal_pairs=[("beta", "alpha")])
    chained = dp.profile(COLUMNS, temporal_pairs=[("alpha", "beta"), ("beta", "gamma")])
    shuffled = dp.profile(
        COLUMNS, temporal_pairs=[("beta", "gamma"), ("alpha", "beta"), ("alpha", "beta")]
    )
    # The same three columns, paired otherwise.
    regrouped = dp.profile(COLUMNS, temporal_pairs=[("alpha", "gamma"), ("beta", "gamma")])
    none = dp.profile(COLUMNS)
    cases = [
        ("same pairs, other order and repetition", shuffled, chained, ["passed", "passed"]),
        ("reversed", reversed_, forward, [CHANGED, CHANGED]),
        ("same columns, other pairs", regrouped, chained, [CHANGED, CHANGED]),
        ("declared on one side", forward, none, [CHANGED, CHANGED]),
        (
            "baseline before 0.14",
            chained,
            dp.ProfileReport.from_dict(_legacy(chained)),
            [UNRECORDED, UNRECORDED],
        ),
        (
            "baseline before 0.14, no pairs",
            none,
            dp.ProfileReport.from_dict(_legacy(none)),
            ["passed", "passed"],
        ),
    ]
    wrong = []
    for label, report, baseline, want in cases:
        for name, side in _sides(baseline):
            got = _gate(report, side)
            if got != want:
                wrong.append(f"[{label}] baseline {name}: {got!r} != {want!r}")
    assert not wrong, "\n".join(wrong)
