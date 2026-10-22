"""Fieldless JSON rows participate in row-level quality metrics (#1021)."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import dataprof
import pytest
from dataprof import asyncio as dpa

requires_async = pytest.mark.skipif(
    not dataprof.capabilities().async_streaming,
    reason="Async streaming not compiled. Build with --features "
    "'python,python-async,async-streaming'.",
)
ROUTES = [
    "file.auto",
    "file.incremental",
    "file.columnar",
    "bytes",
    pytest.param("async_bytes.1", marks=requires_async),
    pytest.param("async_bytes.64", marks=requires_async),
    pytest.param("async_file.1", marks=requires_async),
    pytest.param("async_file.64", marks=requires_async),
]


def _payload(records: list[dict[str, Any]], fmt: str) -> bytes:
    if fmt == "json":
        return json.dumps(records).encode()
    return b"".join(json.dumps(record).encode() + b"\n" for record in records)


def _profile(
    tmp_path: Path, data: bytes, fmt: str, route: str, *, max_rows: int | None = None
) -> dataprof.ProfileReport:
    if route == "bytes":
        return dataprof.profile(data, format=fmt, max_rows=max_rows)

    path = tmp_path / f"records.{fmt}"
    path.write_bytes(data)
    source, option = route.split(".")
    if source == "file":
        return dataprof.profile(path, format=fmt, engine=option, max_rows=max_rows)
    if source == "async_bytes":
        return asyncio.run(
            dpa.profile_bytes(data, format=fmt, chunk_size=int(option), max_rows=max_rows)
        )
    assert source == "async_file"
    return asyncio.run(dpa.profile_file(path, chunk_size=int(option), max_rows=max_rows))


def _measured(report: dataprof.ProfileReport) -> dict[str, Any]:
    """Compare both serialized metric dialects; execution provenance is separate."""
    flat = report.to_dict()
    canonical = json.loads(report.to_json())
    return {
        "columns": flat["columns"],
        "quality": flat["quality"],
        "canonical_columns": canonical["column_profiles"],
        "canonical_quality": canonical["quality"],
    }


def _assert_rows(
    report: dataprof.ProfileReport,
    *,
    rows: int,
    nulls: int,
    complete_ratio: float,
    missing_ratio: float,
    duplicates: int,
) -> None:
    document = report.to_dict()
    assert (report.rows, report.columns) == (rows, 2)
    assert document["execution"]["rows_processed"] == rows
    assert document["execution"]["error_count"] == 0
    assert [
        (column["name"], column["total_count"], column["null_count"])
        for column in document["columns"]
    ] == [("z", rows, nulls), ("a", rows, nulls)]
    completeness = document["quality"]["completeness"]
    assert completeness["total_cells"] == rows * 2
    assert completeness["complete_records_ratio"] == complete_ratio
    assert completeness["missing_values_ratio"] == missing_ratio
    uniqueness = document["quality"]["uniqueness"]
    assert uniqueness["rows_checked"] == rows
    assert uniqueness["duplicate_rows"] == duplicates
    assert uniqueness["duplicate_rows_approximate"] is False


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("fmt", ["json", "jsonl"])
@pytest.mark.parametrize(
    ("records", "rows", "nulls", "complete_ratio", "missing_ratio", "duplicates"),
    [
        pytest.param(
            [{}, {"z": 7, "a": 9}, {"z": 7, "a": 9}],
            3,
            1,
            66.67,
            33.33,
            1,
            id="one-leading-empty",
        ),
        pytest.param(
            [{}, {}, {"z": 7, "a": 9}, {"z": 7, "a": 9}],
            4,
            2,
            50.0,
            50.0,
            2,
            id="multiple-leading-empty",
        ),
        pytest.param(
            [{"z": 7, "a": 9}, {}, {"z": 7, "a": 9}],
            3,
            1,
            66.67,
            33.33,
            1,
            id="empty-after-schema",
        ),
        pytest.param(
            [{}, {"z": None, "a": None}, {"z": 7, "a": 9}],
            3,
            2,
            33.33,
            66.67,
            1,
            id="empty-and-explicit-nulls",
        ),
    ],
)
def test_fieldless_rows_count_in_quality(
    tmp_path, route, fmt, records, rows, nulls, complete_ratio, missing_ratio, duplicates
):
    data = _payload(records, fmt)
    report = _profile(tmp_path, data, fmt, route)
    # The first fixture has two complete records out of three, not one.
    _assert_rows(
        report,
        rows=rows,
        nulls=nulls,
        complete_ratio=complete_ratio,
        missing_ratio=missing_ratio,
        duplicates=duplicates,
    )
    assert report.source_exhausted is True
    assert _measured(report) == _measured(dataprof.profile(data, format=fmt))


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("fmt", ["json", "jsonl"])
def test_all_fieldless_rows_keep_quality_unassessed(tmp_path, route, fmt):
    data = _payload([{}, {}, {}], fmt)
    report = _profile(tmp_path, data, fmt, route)
    document = report.to_dict()
    assert (report.rows, report.columns) == (3, 0)
    assert document["columns"] == []
    assert document["execution"]["rows_processed"] == 3
    assert document["execution"]["error_count"] == 0
    assert report.source_exhausted is True
    assert report.quality_status == "computed"
    assert document["quality"]["overall_score"] is None
    assert document["quality"]["assessed_dimensions"] == []
    assert "completeness" not in document["quality"]
    assert "uniqueness" not in document["quality"]
    assert _measured(report) == _measured(dataprof.profile(data, format=fmt))


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("fmt", ["json", "jsonl"])
@pytest.mark.parametrize("max_rows", [1, 2, 3])
def test_capped_fieldless_prefix_uses_only_consumed_rows(tmp_path, route, fmt, max_rows):
    # A large first chunk contains the unread key, while chunk_size=1 discovers
    # fields incrementally. Both must describe only the consumed prefix.
    records = [{}, {"z": 7, "a": 9}, {"z": 7, "a": 9}, {"unread": 1}]
    data = _payload(records, fmt)
    report = _profile(tmp_path, data, fmt, route, max_rows=max_rows)
    assert report.rows == max_rows
    assert report.error_count == 0
    assert report.source_exhausted is False
    assert _measured(report) == _measured(dataprof.profile(data, format=fmt, max_rows=max_rows))
    if max_rows == 1:
        assert report.columns == 0
        assert report.quality_score is None
        assert report.to_dict()["quality"]["assessed_dimensions"] == []
    else:
        _assert_rows(
            report,
            rows=max_rows,
            nulls=1,
            complete_ratio=50.0 if max_rows == 2 else 66.67,
            missing_ratio=50.0 if max_rows == 2 else 33.33,
            duplicates=max_rows - 2,
        )
