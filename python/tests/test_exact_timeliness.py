"""Exact timeliness from the date statistics reader over every scanned value (#788 step 4).

Rust twin: ``tests/exact_timeliness.rs``.

Covers CSV (auto, incremental, columnar), CSV bytes, JSON, JSONL, Parquet,
Arrow tables and streams, pandas, polars (when installed), Python dicts, list
of dicts, and quality-only runs at 40,000 rows with the dates that matter past
the 10,000-value sample, plus the #1422 ``exact_dimensions`` /
``sampled_dimensions`` parity check for ``timeliness``.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import dataprof as dp
import pytest

pa = pytest.importorskip("pyarrow")
pd = pytest.importorskip("pandas")

ROWS = 40_000
REFERENCE = "2024-06-01"


def _fixture_columns() -> dict[str, list[str | None]]:
    event_date: list[str | None] = ["2024-03-15"] * ROWS
    for i in range(39_700, 39_800):
        event_date[i] = "2025-01-10"  # 100 future dates past sample
    for i in range(39_800, 39_900):
        event_date[i] = "2015-01-01"  # 100 stale dates past sample
    for i in range(39_900, 39_950):
        event_date[i] = " 2024-04-01 "  # 50 whitespace-padded valid dates
    for i in range(39_950, 39_980):
        event_date[i] = "2024-4-5"  # 30 single-digit valid dates
    for i in range(39_980, ROWS):
        event_date[i] = "not-a-date"  # 20 invalid dates past sample

    start_date: list[str | None] = ["01/05/2024"] * ROWS
    end_date: list[str | None] = ["02/03/2024"] * ROWS
    start_date[0] = None  # null start skipped without shifting partner rows (#787)
    end_date[1] = None  # null end skipped without shifting partner rows (#787)
    for i in range(39_950, 39_999):
        start_date[i] = "03/01/2024"  # 49 month-first inversions past sample
        end_date[i] = "02/01/2024"
    start_date[39_999] = "12/30/2023"  # settles MonthFirst past sample (#1305)
    end_date[39_999] = "12/31/2023"

    mixed_hinted: list[str | None] = ["text"] * ROWS
    for i in range(39_900, 39_999):
        mixed_hinted[i] = "12/01/2024"  # 99 future dates once MonthFirst settled
    mixed_hinted[39_999] = "12/31/2023"

    valid_from: list[str | None] = ["pending"] * ROWS
    valid_until: list[str | None] = ["pending"] * ROWS
    for i in range(39_900, 39_990):
        valid_from[i] = "01/05/2024"
        valid_until[i] = "02/03/2024"
    for i in range(39_990, 39_999):
        valid_from[i] = "05/01/2024"  # 9 month-first inversions
        valid_until[i] = "04/01/2024"
    valid_from[39_999] = "12/30/2023"
    valid_until[39_999] = "12/31/2023"

    return {
        "event_date": event_date,
        "start_date": start_date,
        "end_date": end_date,
        "mixed_hinted": mixed_hinted,
        "valid_from": valid_from,
        "valid_until": valid_until,
    }


def _csv_text(columns: dict[str, list[str | None]]) -> str:
    names = list(columns)
    lines = [",".join(names)]
    for row in zip(*(columns[n] for n in names), strict=True):
        lines.append(",".join("" if v is None else v for v in row))
    return "\n".join(lines) + "\n"


def _records(columns: dict[str, list[str | None]]) -> list[dict[str, str | None]]:
    names = list(columns)
    return [
        dict(zip(names, row, strict=True)) for row in zip(*(columns[n] for n in names), strict=True)
    ]


def _all_routes(
    tmp_path: Path,
    columns: dict[str, list[str | None]],
    kwargs: dict[str, Any],
) -> dict[str, dp.ProfileReport]:
    csv_text = _csv_text(columns)
    csv_path = tmp_path / "timeliness.csv"
    csv_path.write_text(csv_text, encoding="utf-8")

    records = _records(columns)
    json_path = tmp_path / "timeliness.json"
    json_path.write_text(json.dumps(records), encoding="utf-8")
    jsonl_path = tmp_path / "timeliness.jsonl"
    jsonl_path.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")

    table = pa.table({k: pa.array(v, type=pa.string()) for k, v in columns.items()})
    parquet_path = tmp_path / "timeliness.parquet"
    import pyarrow.parquet as pq

    pq.write_table(table, parquet_path)

    class _ArrowStream:
        def __arrow_c_stream__(self, requested_schema=None):
            return table.to_reader(max_chunksize=8_192).__arrow_c_stream__(requested_schema)

    runners: dict[str, Callable[[], dp.ProfileReport]] = {
        "csv_auto": lambda: dp.profile(str(csv_path), engine="auto", **kwargs),
        "csv_incremental": lambda: dp.profile(str(csv_path), engine="incremental", **kwargs),
        "csv_columnar": lambda: dp.profile(str(csv_path), engine="columnar", **kwargs),
        "csv_quality_only": lambda: dp.profile(
            str(csv_path), engine="incremental", metrics=["quality"], **kwargs
        ),
        "csv_bytes": lambda: dp.profile(csv_text.encode("utf-8"), format="csv", **kwargs),
        "json": lambda: dp.profile(str(json_path), **kwargs),
        "jsonl": lambda: dp.profile(str(jsonl_path), **kwargs),
        "parquet": lambda: dp.profile(str(parquet_path), **kwargs),
        "parquet_bytes": lambda: dp.profile(parquet_path.read_bytes(), format="parquet", **kwargs),
        "arrow_table": lambda: dp.profile(table, **kwargs),
        "arrow_stream": lambda: dp.profile(_ArrowStream(), **kwargs),
        "pandas": lambda: dp.profile(pd.DataFrame(columns), **kwargs),
        "dict": lambda: dp.profile(columns, **kwargs),
        "list_of_dicts": lambda: dp.profile(records, **kwargs),
    }
    try:
        import polars as pl

        runners["polars"] = lambda: dp.profile(pl.from_arrow(table), **kwargs)
    except ImportError:
        pass

    return {name: run() for name, run in runners.items()}


def _timeliness_view(report: dp.ProfileReport) -> dict[str, Any]:
    doc = json.loads(report.to_json())
    return {
        "metrics": doc["quality"]["metrics"]["timeliness"],
        "score": doc["quality"]["scores"]["dimension_scores"]["timeliness"],
    }


def test_every_route_counts_timeliness_over_every_value(tmp_path: Path) -> None:
    columns = _fixture_columns()
    kwargs = {
        "reference_date": REFERENCE,
        "temporal_columns": ["mixed_hinted"],
        "temporal_pairs": [("valid_from", "valid_until")],
    }
    reports = _all_routes(tmp_path, columns, kwargs)
    reference = _timeliness_view(reports["dict"])
    expected_metrics = (239_998, 119_720, 199, 40_098, 58)

    wrong = []
    for name, report in reports.items():
        view = _timeliness_view(report)
        m = view["metrics"]
        got = (
            m["date_values_checked"],
            m["invalid_date_values"],
            m["future_dates_count"],
            m["temporal_pairs_checked"],
            m["temporal_violations"],
        )
        if got != expected_metrics:
            wrong.append(f"[{name}] metrics={got} != {expected_metrics}")
        if view != reference:
            wrong.append(f"[{name}] serialized timeliness differs: {view} != {reference}")
        q = report.to_dict()["quality"]
        if "timeliness" not in q["exact_dimensions"] or "timeliness" in q["sampled_dimensions"]:
            wrong.append(
                f"[{name}] exact={q['exact_dimensions']} sampled={q['sampled_dimensions']}"
            )
        semantics = report.metric_semantics or {}
        if (
            semantics.get("value_facts")
            != "every_value_consistency_precision_accuracy_rules_timeliness"
        ):
            wrong.append(f"[{name}] value_facts={semantics.get('value_facts')!r}")
    assert not wrong, "\n".join(wrong)


def test_issue_1422_timeliness_exact_and_sampled_dimensions_match(tmp_path: Path) -> None:
    """#1422: file, bytes, dict, Arrow, pandas agree that timeliness is exact."""
    columns: dict[str, list[str | None]] = {
        "start_date": ["01/05/2024"] * (ROWS - 1) + ["12/30/2023"],
        "end_date": ["02/03/2024"] * (ROWS - 1) + ["12/31/2023"],
    }
    reports = _all_routes(tmp_path, columns, {"reference_date": REFERENCE})
    reference = _timeliness_view(reports["dict"])

    wrong = []
    for name, report in reports.items():
        q = report.to_dict()["quality"]
        if "timeliness" not in q["exact_dimensions"]:
            wrong.append(
                f"[{name}] timeliness missing from exact_dimensions: {q['exact_dimensions']}"
            )
        if "timeliness" in q["sampled_dimensions"]:
            wrong.append(
                f"[{name}] timeliness still in sampled_dimensions: {q['sampled_dimensions']}"
            )
        if _timeliness_view(report) != reference:
            wrong.append(f"[{name}] timeliness differs from dict")
    assert not wrong, "\n".join(wrong)
