"""Column-ordering contract across formats and transports (#465).

Columns are reported in source order: CSV header order, Parquet schema order,
and for JSON/JSONL the first record's field order with fields that only appear
in later records appended where they were first seen. Reordering is invisible in
value-based correctness tests but shows up in every report the user reads, so it
needs its own guard on every transport.

The fixture field names are deliberately non-alphabetical — sorting them yields
``active, amount, date, id``, which is what JSON profiling used to return.
"""

from __future__ import annotations

import asyncio
import json

import dataprof
import pytest
from dataprof.asyncio import profile_bytes, profile_file

_HAS_ASYNC = dataprof.capabilities().async_streaming
requires_async = pytest.mark.skipif(
    not _HAS_ASYNC,
    reason="Async streaming not compiled. Build with --features "
    "'python,python-async,async-streaming'.",
)

SOURCE_ORDER = ["id", "amount", "active", "date"]

ROWS = [
    {"id": 1, "amount": 12.5, "active": True, "date": "2026-07-23"},
    {"id": 2, "amount": 7.25, "active": False, "date": "2026-07-24"},
]

PAYLOADS = {
    "csv": b"id,amount,active,date\n1,12.5,true,2026-07-23\n2,7.25,false,2026-07-24\n",
    "json": json.dumps(ROWS).encode("utf-8"),
    "jsonl": ("\n".join(json.dumps(row) for row in ROWS) + "\n").encode("utf-8"),
}


def _write(tmp_path, fmt: str):
    path = tmp_path / f"fixture.{fmt}"
    path.write_bytes(PAYLOADS[fmt])
    return str(path)


@pytest.mark.parametrize("fmt", ["csv", "json", "jsonl"])
def test_file_input_preserves_source_column_order(fmt, tmp_path):
    report = dataprof.profile(_write(tmp_path, fmt))
    assert list(report) == SOURCE_ORDER


@pytest.mark.parametrize("fmt", ["csv", "json", "jsonl"])
def test_bytes_input_preserves_source_column_order(fmt):
    report = dataprof.profile(PAYLOADS[fmt], format=fmt)
    assert list(report) == SOURCE_ORDER


def test_record_input_preserves_source_column_order():
    assert list(dataprof.profile(ROWS)) == SOURCE_ORDER


def test_single_json_object_preserves_source_field_order():
    # A lone root object profiles as one row; its fields are the columns.
    payload = json.dumps(ROWS[0]).encode("utf-8")
    assert list(dataprof.profile(payload, format="json")) == SOURCE_ORDER


def test_late_fields_are_appended_in_first_seen_order():
    # Each record introduces two fields in reverse-alphabetical order, so
    # sorting would reshuffle both the leading and the appended pair.
    rows = [{"zulu": 1, "mike": 2}, {"zulu": 3, "mike": 4, "delta": 5, "alpha": 6}]
    payload = ("\n".join(json.dumps(row) for row in rows) + "\n").encode("utf-8")

    assert list(dataprof.profile(payload, format="jsonl")) == ["zulu", "mike", "delta", "alpha"]


@pytest.mark.parametrize("fmt", ["csv", "json", "jsonl"])
def test_file_and_bytes_transports_agree_on_column_order(fmt, tmp_path):
    from_file = dataprof.profile(_write(tmp_path, fmt))
    from_bytes = dataprof.profile(PAYLOADS[fmt], format=fmt)
    assert list(from_file) == list(from_bytes)


@requires_async
@pytest.mark.parametrize("fmt", ["csv", "json", "jsonl"])
def test_async_transports_preserve_source_column_order(fmt, tmp_path):
    path = _write(tmp_path, fmt)
    from_file = asyncio.run(profile_file(path))
    from_bytes = asyncio.run(profile_bytes(PAYLOADS[fmt], format=fmt))

    assert list(from_file) == SOURCE_ORDER, f"{fmt}: async file"
    assert list(from_bytes) == SOURCE_ORDER, f"{fmt}: async bytes"


def test_to_dict_export_keeps_report_column_order(tmp_path):
    report = dataprof.profile(_write(tmp_path, "json"))
    exported = [column["name"] for column in report.to_dict()["columns"]]
    assert exported == SOURCE_ORDER
