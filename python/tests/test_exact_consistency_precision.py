"""Consistency and precision are counted over every scanned value (#788 step 2).

At 40,000 rows with minority type violations, mixed date formats, mixed decimal
separators, encoding issues, date-named digit codes and minority decimal scales
placed past the 10,000-value sample, every route (dict, bytes, CSV engines,
JSON/JSONL, Parquet, Arrow, Arrow C stream, pandas, polars) serializes equal
consistency and precision blocks, places both dimensions in ``exact_dimensions``
and leaves ``sampled_dimensions`` without them.

``tests/exact_consistency_precision.rs`` is the Rust twin.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

try:
    import dataprof as dp
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

ROWS = 40_000


def _columns() -> dict[str, list[str]]:
    code = [str(1 + i % 900) for i in range(ROWS)]
    for i in range(35_000, 35_200):
        code[i] = "junk_token"

    amount = [f"{10 + (i % 90)}.{1 + (i % 9)}{1 + (i % 8)}" for i in range(ROWS)]
    for i in range(39_500, ROWS):
        amount[i] = "12.3456"

    event_date = [f"2024-03-{1 + (i % 28):02d}" for i in range(ROWS)]
    for i in range(39_700, ROWS):
        event_date[i] = "15/03/2024"

    note = [f"item_{i}" for i in range(ROWS)]
    for i in range(39_800, ROWS):
        note[i] = "Caf\u00c3\u00a9"

    sep = [f"v{i % 100}.5" for i in range(ROWS)]
    for i in range(39_800, 39_900):
        sep[i] = "v1,5"
    for i in range(39_900, ROWS):
        sep[i] = "1,234.56"

    departure_time = ["1045"] * ROWS
    for i in range(39_600, ROWS):
        departure_time[i] = "0930"

    return {
        "code": code,
        "amount": amount,
        "event_date": event_date,
        "note": note,
        "sep": sep,
        "departure_time": departure_time,
    }


def _csv_text(columns: dict[str, list[str]]) -> str:
    names = list(columns)
    rows = zip(*(columns[name] for name in names), strict=True)
    return ";".join(names) + "\n" + "".join(";".join(row) + "\n" for row in rows)


def _json_lines(columns: dict[str, list[str]]) -> list[str]:
    names = list(columns)
    return [
        json.dumps(dict(zip(names, row, strict=True)), ensure_ascii=False)
        for row in zip(*(columns[name] for name in names), strict=True)
    ]


def _routes(tmp_path: Path) -> dict[str, Callable[[], dp.ProfileReport]]:
    columns = _columns()
    csv_text = _csv_text(columns)
    csv_path = tmp_path / "data.csv"
    csv_path.write_text(csv_text, encoding="utf-8")

    lines = _json_lines(columns)
    jsonl_text = "\n".join(lines) + "\n"
    json_text = "[" + ",".join(lines) + "]"
    jsonl_path = tmp_path / "data.jsonl"
    jsonl_path.write_text(jsonl_text, encoding="utf-8")
    json_path = tmp_path / "data.json"
    json_path.write_text(json_text, encoding="utf-8")

    routes: dict[str, Callable[[], dp.ProfileReport]] = {
        "dict": lambda: dp.profile(columns),
        "csv_bytes": lambda: dp.profile(csv_text.encode("utf-8"), format="csv"),
        "jsonl_bytes": lambda: dp.profile(jsonl_text.encode("utf-8"), format="jsonl"),
        "json_bytes": lambda: dp.profile(json_text.encode("utf-8"), format="json"),
        "file_json": lambda: dp.profile(str(json_path)),
        "file_jsonl": lambda: dp.profile(str(jsonl_path)),
    }
    for engine in ("auto", "incremental", "columnar"):
        routes[f"file_{engine}"] = lambda engine=engine: dp.profile(str(csv_path), engine=engine)

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.table(columns)
        pq_path = tmp_path / "data.parquet"
        pq.write_table(table, pq_path)
        routes["parquet"] = lambda: dp.profile(str(pq_path))
        routes["arrow"] = lambda: dp.profile(table)
        routes["arrow_stream"] = lambda: dp.profile(
            pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
        )
    except ImportError:
        pass

    try:
        import pandas as pd

        routes["pandas"] = lambda: dp.profile(pd.DataFrame(columns))
    except ImportError:
        pass

    try:
        import polars as pl

        routes["polars"] = lambda: dp.profile(pl.DataFrame(columns))
    except ImportError:
        pass

    return routes


def test_every_route_matches_consistency_and_precision_at_40k_rows(tmp_path: Path) -> None:
    wrong: list[str] = []
    baseline: dict[str, Any] | None = None

    for label, run in _routes(tmp_path).items():
        report = run()
        doc = json.loads(report.to_json())
        summary = report.to_dict()
        restored = dp.ProfileReport.from_dict(summary)

        semantics = report.metric_semantics or {}
        if semantics.get("value_facts") != "every_value_consistency_precision_accuracy_rules":
            wrong.append(f"[{label}] value_facts={semantics.get('value_facts')!r}")

        q = doc["quality"]
        cons = q["metrics"]["consistency"]
        prec = q["metrics"]["precision"]
        scores = q["scores"]["dimension_scores"]

        expected_cons = {
            "data_type_consistency": 99.92,
            "format_violations": 500,
            "encoding_issues": 200,
            "values_checked": 240_000,
        }
        expected_prec = {
            "decimal_places_consistency": 99.37,
            "inconsistent_precision_values": 500,
            "numeric_values_checked": 79_800,
        }
        if cons != expected_cons:
            wrong.append(f"[{label}] consistency={cons} != {expected_cons}")
        if prec != expected_prec:
            wrong.append(f"[{label}] precision={prec} != {expected_prec}")

        for form, checked_report in (("live", report), ("summary", restored)):
            sampled = checked_report.quality_sampled_dimensions
            exact = checked_report.to_dict()["quality"].get("exact_dimensions", [])
            if sampled is None:
                wrong.append(f"[{label}:{form}] sampled_dimensions is None")
            else:
                for dim in ("consistency", "precision"):
                    if dim in sampled:
                        wrong.append(f"[{label}:{form}] {dim} still in sampled={sampled}")
                    if dim not in exact:
                        wrong.append(f"[{label}:{form}] {dim} missing from exact={exact}")

        bounds = q.get("score_bounds")
        if bounds is not None:
            for dim in ("consistency", "precision"):
                interval = bounds["dimension_scores"][dim]
                lower, upper = interval["lower"], interval["upper"]
                score = scores[dim]
                if not (lower <= score <= upper and (upper - lower) <= 0.01 + 1e-9):
                    wrong.append(f"[{label}] score_bounds[{dim}]={interval} not exact for {score}")

        snap = {
            "consistency": cons,
            "precision": prec,
            "consistency_score": scores["consistency"],
            "precision_score": scores["precision"],
        }
        if baseline is None:
            baseline = snap
        elif snap != baseline:
            wrong.append(f"[{label}] {snap} != baseline {baseline}")

    assert not wrong, "mismatches:\n" + "\n".join(wrong)


def test_typed_parquet_float64_matches_csv_at_40k_rows(tmp_path: Path) -> None:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    ids = list(range(ROWS))
    prices = [12.3456 if i >= 39_500 else 12.34 for i in range(ROWS)]

    csv_path = tmp_path / "prices.csv"
    csv_path.write_text(
        "id,price\n" + "".join(f"{i},{p}\n" for i, p in zip(ids, prices, strict=True)),
        encoding="utf-8",
    )
    pq_path = tmp_path / "prices.parquet"
    pq.write_table(
        pa.table(
            {"id": pa.array(ids, type=pa.int64()), "price": pa.array(prices, type=pa.float64())}
        ),
        pq_path,
    )

    csv_doc = json.loads(dp.profile(str(csv_path)).to_json())
    pq_doc = json.loads(dp.profile(str(pq_path)).to_json())

    assert (
        pq_doc["quality"]["metrics"]["consistency"] == csv_doc["quality"]["metrics"]["consistency"]
    )
    assert pq_doc["quality"]["metrics"]["precision"] == csv_doc["quality"]["metrics"]["precision"]
    assert pq_doc["quality"]["metrics"]["precision"] == {
        "decimal_places_consistency": 98.75,
        "inconsistent_precision_values": 500,
        "numeric_values_checked": ROWS,
    }


def test_exact_only_dimensions_omit_score_bounds_and_partial_read_stays_exact(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "data.csv"
    csv_path.write_text(_csv_text(_columns()), encoding="utf-8")

    exact_only = dp.profile(
        str(csv_path),
        quality_dimensions=["completeness", "consistency", "precision"],
    )
    assert exact_only.quality_score_bounds is None
    assert exact_only.quality_sampled_dimensions == []
    exact = exact_only.to_dict()["quality"]["exact_dimensions"]
    assert set(exact) == {"completeness", "consistency", "precision"}

    capped = dp.profile(str(csv_path), max_rows=15_000)
    assert "consistency" not in (capped.quality_sampled_dimensions or [])
    assert "precision" not in (capped.quality_sampled_dimensions or [])
    capped_exact = capped.to_dict()["quality"]["exact_dimensions"]
    assert "consistency" in capped_exact
    assert "precision" in capped_exact
    assert capped.quality is not None
    assert capped.quality.consistency is not None
    assert capped.quality.consistency["values_checked"] == 15_000 * 6


def test_dict_with_null_in_every_column_keeps_consistency_and_precision_exact() -> None:
    # When every column has at least one null, `columns.rs` hands `ReportAssembler`
    # fewer non-null values than `rows_processed`, entering the bifurcated path.
    # Feeding `LexicalTally` there keeps consistency and precision exact.
    data = {
        "a": [None, "1.2", "3.4", "5.67"],
        "b": ["ok", None, "Caf\u00c3\u00a9", "ok"],
    }
    report = dp.profile(data)
    sampled = report.quality_sampled_dimensions
    exact = report.to_dict()["quality"]["exact_dimensions"]
    assert sampled is not None
    assert "consistency" not in sampled
    assert "precision" not in sampled
    assert "consistency" in exact
    assert "precision" in exact
    assert report.quality is not None
    assert report.quality.consistency is not None
    assert report.quality.consistency["encoding_issues"] == 1
    assert report.quality.precision is not None
    assert report.quality.precision["inconsistent_precision_values"] == 1
