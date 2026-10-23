"""Reports record the columns they were profiled with (#749).

``tests/column_selection_record.rs`` is the Rust twin. Here the extra routes
are the Python ones: in-memory inputs, DataFrames, Arrow, Parquet, and a flat
``to_dict()`` summary.
"""

import json
from pathlib import Path
from typing import Any

import dataprof as dp
import pytest

ORDERS = {"id": [1, 2, 3], "amount": [10, 20, 30], "note": ["a", "b", "c"]}
ALL = {"selection": "all"}
LISTED = {"selection": "listed", "names": ["note", "id"]}


def _routes(tmp_path: Path) -> dict[str, Any]:
    csv = tmp_path / "orders.csv"
    csv.write_text("id,amount,note\n1,10,a\n2,20,b\n3,30,c\n")
    rows = [dict(zip(ORDERS, values)) for values in zip(*ORDERS.values())]
    jsonl = tmp_path / "orders.jsonl"
    jsonl.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
    routes: dict[str, Any] = {
        "dict": ORDERS,
        "rows": rows,
        "csv auto": (str(csv), "auto"),
        "csv incremental": (str(csv), "incremental"),
        "csv columnar": (str(csv), "columnar"),
        "jsonl": str(jsonl),
    }
    # Optional packages add routes; without them the core routes still run.
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        pass
    else:
        routes["arrow"] = pa.table(ORDERS)
        parquet = tmp_path / "orders.parquet"
        pq.write_table(pa.table(ORDERS), parquet)
        routes["parquet"] = str(parquet)
    try:
        import pandas as pd
    except ImportError:
        pass
    else:
        routes["pandas"] = pd.DataFrame(ORDERS)
    return routes


def _profile(source: Any, **options: Any) -> dp.ProfileReport:
    if isinstance(source, tuple):
        path, engine = source
        return dp.profile(path, engine=engine, **options)
    return dp.profile(source, **options)


def test_every_input_path_records_the_selection(tmp_path: Path):
    wrong = []
    for name, source in _routes(tmp_path).items():
        for packs in ({}, {"metrics": ["schema"]}):
            for options, expected in (({}, ALL), ({"columns": ["note", "id"]}, LISTED)):
                settings = _profile(source, **packs, **options).analysis_settings
                if settings is None or settings["columns"] != expected:
                    wrong.append((name, packs, options, settings))
    assert wrong == []


def test_a_document_keeps_the_selection(tmp_path: Path):
    report = dp.profile(ORDERS, columns=["note", "id"])
    assert report.analysis_settings is not None
    assert report.analysis_settings["columns"] == LISTED
    # A copy: changing it does not change the report.
    report.analysis_settings["columns"]["names"].append("amount")
    assert report.analysis_settings["columns"] == LISTED

    saved = tmp_path / "report.json"
    report.save(saved)
    loaded = dp.ProfileReport.load(saved).analysis_settings
    assert loaded is not None and loaded["columns"] == LISTED
    summary = report.to_dict()
    assert summary["analysis_settings"]["columns"] == LISTED
    flat = dp.ProfileReport.from_dict(summary)
    assert flat.analysis_settings is not None
    assert flat.analysis_settings["columns"] == LISTED
    flat.analysis_settings["columns"]["names"].append("amount")
    assert flat.analysis_settings["columns"] == LISTED

    for malformed in (
        None,
        {},
        {"selection": "listed"},
        {"selection": "some"},
        {"selection": "listed", "names": "id"},
        {"selection": "listed", "names": [1]},
    ):
        broken = json.loads(json.dumps(summary))
        broken["analysis_settings"]["columns"] = malformed
        with pytest.raises(ValueError, match="analysis_settings.columns"):
            dp.ProfileReport.from_dict(broken)


def _changes(result) -> list[tuple[str, str, str | None]]:
    assert result.baseline is not None
    changes = []
    for item in result.baseline["diagnostics"]:
        if "setting" in item:
            assert item["setting"] == "columns"
        changes.append((item["code"], item["column"], item.get("reason")))
    return changes


def test_the_side_without_a_column_says_whether_it_selected_it():
    baseline = dp.profile(ORDERS)
    policy: dict[str, Any] = {"max_null_percentage_increase": {"id": 0}}

    without_note = dp.profile({"id": [1, 2, 3], "amount": [10, 20, 30]})
    assert _changes(without_note.check(baseline=baseline, **policy)) == [
        ("column_removed", "note", None)
    ]
    projected = dp.profile(ORDERS, columns=["id", "amount"])
    assert _changes(projected.check(baseline=baseline, **policy)) == [
        ("column_removed", "note", "setting_changed")
    ]
    # The flat summary of a projected run, as a baseline.
    flat = dp.ProfileReport.from_dict(projected.to_dict())
    assert _changes(baseline.check(baseline=flat, **policy)) == [
        ("column_added", "note", "setting_changed")
    ]
    # A summary saved before the record.
    legacy = projected.to_dict()
    del legacy["analysis_settings"]
    assert _changes(baseline.check(baseline=dp.ProfileReport.from_dict(legacy), **policy)) == [
        ("column_added", "note", "setting_unrecorded")
    ]
