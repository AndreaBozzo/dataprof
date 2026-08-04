"""Shape policy for JSON records with no fields, across transports (#463).

A JSON object with no fields is a record: it was read and analysed, and nothing
was found in it. It therefore profiles as a row against zero columns, which is
what the file scanner has always reported. Three shapes stay distinct:

- ``rows > 0, columns == 0`` -- rows with no fields;
- ``rows == 0, columns == 0`` -- an input with no records at all (``[]``);
- ``rows == 0, columns > 0`` -- a known schema with no rows.

Every input path applies that policy: file, synchronous bytes, async bytes,
Python list-of-dicts, and (in ``test_async_url_api.py``) URL. Collapsing a
fieldless row into "no rows" would erase records from the count, and rejecting
it would make the same data profileable from a file but not from bytes.

Distinct from #495 (records that are valid JSON but not objects) and #486
(physical record boundaries).

The async cases are skipped when the extension is built without async support.

Run after building the extension:
    maturin develop --features python,python-async,async-streaming
    pytest python/tests/test_json_fieldless_records.py -v
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import dataprof
import pytest

_HAS_ASYNC = dataprof.capabilities().async_streaming
requires_async = pytest.mark.skipif(
    not _HAS_ASYNC,
    reason="Async streaming not compiled. Build with --features "
    "'python,python-async,async-streaming'.",
)


def _write(tmp_path: Path, data: bytes, suffix: str) -> str:
    target = tmp_path / f"data.{suffix}"
    target.write_bytes(data)
    return str(target)


def _async_bytes(data: bytes, fmt: str, **kwargs):
    from dataprof.asyncio import profile_bytes

    async def _inner():
        return await profile_bytes(data, format=fmt, **kwargs)

    return asyncio.run(_inner())


# (id, payload, format, expected rows, expected columns)
#
# `format="json"` is the array grammar on the async reader and the object-or-
# array grammar elsewhere, so a bare `{}` document is listed under "jsonl",
# where every transport agrees on how records are delimited. Reconciling the
# format hints themselves is #486.
SHAPE_CASES = [
    ("one fieldless object", b"{}", "jsonl", 1, 0),
    ("two fieldless objects", b"{}\n{}\n", "jsonl", 2, 0),
    ("array of one fieldless", b"[{}]", "json", 1, 0),
    ("array of two fieldless", b"[{},{}]", "json", 2, 0),
    ("empty array", b"[]", "json", 0, 0),
    ("fieldless then fielded", b'[{},{"a":1}]', "json", 2, 1),
    ("fielded then fieldless", b'[{"a":1},{}]', "json", 2, 1),
    ("fieldless among fielded", b'{"a":1}\n{}\n{"a":2}\n', "jsonl", 3, 1),
]

SHAPE_IDS = [case[0] for case in SHAPE_CASES]


@pytest.mark.parametrize(
    ("payload", "fmt", "rows", "columns"),
    [case[1:] for case in SHAPE_CASES],
    ids=SHAPE_IDS,
)
def test_file_reports_fieldless_rows(tmp_path, payload, fmt, rows, columns):
    report = dataprof.profile(_write(tmp_path, payload, fmt), format=fmt)
    assert (report.rows, report.columns) == (rows, columns)
    assert report.error_count == 0


@pytest.mark.parametrize(
    ("payload", "fmt", "rows", "columns"),
    [case[1:] for case in SHAPE_CASES],
    ids=SHAPE_IDS,
)
def test_bytes_match_the_file_shape(payload, fmt, rows, columns):
    report = dataprof.profile(payload, format=fmt)
    assert (report.rows, report.columns) == (rows, columns)
    assert report.error_count == 0


@requires_async
@pytest.mark.parametrize(
    ("payload", "fmt", "rows", "columns"),
    [case[1:] for case in SHAPE_CASES],
    ids=SHAPE_IDS,
)
def test_async_bytes_match_the_file_shape(payload, fmt, rows, columns):
    report = _async_bytes(payload, fmt)
    assert (report.rows, report.columns) == (rows, columns)
    assert report.error_count == 0


@pytest.mark.parametrize(
    ("records", "rows", "columns"),
    [
        ([{}], 1, 0),
        ([{}, {}], 2, 0),
        ([], 0, 0),
        ([{}, {"a": 1}], 2, 1),
        ([{"a": 1}, {}], 2, 1),
    ],
    ids=["one", "two", "empty list", "fieldless first", "fieldless last"],
)
def test_python_records_match_the_file_shape(records, rows, columns):
    report = dataprof.profile(records)
    assert (report.rows, report.columns) == (rows, columns)


# --- A root `{}` is a record, not an empty map of columns -------------------


def test_root_object_bytes_is_a_record_not_an_empty_column_map(tmp_path):
    """`{}` decodes to an empty mapping, and "every value is a list of cells"
    is vacuously true of it, so it reads as a column-oriented document holding
    no rows unless the record interpretation wins. The file scanner reads a root
    object as one record, so the bytes path must too.
    """
    from_bytes = dataprof.profile(b"{}", format="json")
    from_file = dataprof.profile(_write(tmp_path, b"{}", "json"))

    assert (from_bytes.rows, from_bytes.columns) == (1, 0)
    assert (from_file.rows, from_file.columns) == (1, 0)


@pytest.mark.parametrize(
    ("payload", "rows", "columns"),
    [
        (b'{"a":[1,2]}', 2, 1),
        (b'{"a":[1,2],"b":[3,4]}', 2, 2),
        (b'{"a":1}', 1, 1),
    ],
    ids=["one column", "two columns", "single record"],
)
def test_populated_root_objects_keep_their_reading(payload, rows, columns):
    """The empty-object carve-out must not change how populated roots are read."""
    report = dataprof.profile(payload, format="json")
    assert (report.rows, report.columns) == (rows, columns)


# --- The three empty-ish shapes stay distinguishable ------------------------


def test_rows_with_no_fields_differ_from_an_input_with_no_records(tmp_path):
    fieldless = dataprof.profile(_write(tmp_path, b"[{},{}]", "json"), format="json")
    no_records = dataprof.profile(_write(tmp_path, b"[]", "json"), format="json")

    assert (fieldless.rows, fieldless.columns) == (2, 0)
    assert (no_records.rows, no_records.columns) == (0, 0)


def test_rows_with_no_fields_differ_from_a_schema_with_no_rows(tmp_path):
    """A CSV header with no data rows is the mirror image: schema, no rows."""
    schema_only = dataprof.profile(b"a,b\n", format="csv")
    fieldless = dataprof.profile(b"{}\n", format="jsonl")

    assert (schema_only.rows, schema_only.columns) == (0, 2)
    assert (fieldless.rows, fieldless.columns) == (1, 0)


# --- A zero-column report is a real report ----------------------------------


def test_zero_column_report_serialises_and_round_trips(tmp_path):
    report = dataprof.profile(_write(tmp_path, b"[{},{}]", "json"), format="json")
    payload = report.to_dict()

    assert payload["columns"] == []
    assert payload["execution"]["rows_processed"] == 2

    restored = dataprof.ProfileReport.from_dict(payload)
    assert (restored.rows, restored.columns) == (2, 0)


def test_zero_column_report_renders_without_a_schema(tmp_path):
    report = dataprof.profile(_write(tmp_path, b"[{},{}]", "json"), format="json")

    # `len()` counts columns, so it is 0 while the report still holds 2 rows.
    assert len(report) == 0
    assert list(report.column_profiles) == []
    assert "rows=2" in repr(report)
    assert report.to_markdown()
    assert report.to_html()


def test_max_rows_caps_fieldless_rows_and_reports_truncation():
    report = dataprof.profile(b"{}\n{}\n{}\n", format="jsonl", max_rows=2)

    assert (report.rows, report.columns) == (2, 0)
    assert not report.source_exhausted


# --- Fieldless is not malformed ---------------------------------------------


@pytest.mark.parametrize("policy", ["skip", "strict"])
@pytest.mark.parametrize("fmt", ["json", "jsonl"])
def test_fieldless_records_are_clean_under_both_error_policies(tmp_path, fmt, policy):
    """`{}` is a well-formed object, so strict mode has nothing to reject."""
    payload = b"[{},{}]" if fmt == "json" else b"{}\n{}\n"
    report = dataprof.profile(_write(tmp_path, payload, fmt), format=fmt, jsonl_on_error=policy)

    assert (report.rows, report.columns) == (2, 0)
    assert report.error_count == 0


@pytest.mark.parametrize("fmt", ["json", "jsonl"])
def test_malformed_input_still_fails_on_every_path(tmp_path, fmt):
    """Widening the shape policy must not turn broken input into an empty profile."""
    payload = b"[{},nope]" if fmt == "json" else b"{}\nnope\n"

    with pytest.raises(ValueError):
        dataprof.profile(_write(tmp_path, payload, fmt), format=fmt, jsonl_on_error="strict")
    with pytest.raises(ValueError):
        dataprof.profile(payload, format=fmt, jsonl_on_error="strict")


def test_a_fieldless_record_does_not_mask_a_malformed_one(tmp_path):
    """Tolerant mode counts the malformed record while the fieldless rows profile."""
    payload = b"{}\nnope\n{}\n"
    for report in (
        dataprof.profile(_write(tmp_path, payload, "jsonl"), format="jsonl"),
        dataprof.profile(payload, format="jsonl"),
    ):
        assert (report.rows, report.columns) == (2, 0)
        assert report.error_count == 1


def test_input_of_only_non_object_records_still_fails(tmp_path):
    """`[1,2]` has no records to profile; `[{},{}]` has two. They must not merge."""
    with pytest.raises(ValueError, match="No valid JSON records|no valid JSON records"):
        dataprof.profile(_write(tmp_path, b"[1,2]", "json"), format="json")

    report = dataprof.profile(_write(tmp_path, b"[{},{}]", "json"), format="json")
    assert (report.rows, report.columns) == (2, 0)
