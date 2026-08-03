"""Non-object JSON record policy across file, bytes, and async inputs (#495).

Only JSON objects are profileable records — they are the only JSON value with
named fields to turn into columns. A record that is valid JSON but not an
object is never silently discarded, because dropping it would turn the source
into a smaller, clean-looking dataset:

- tolerant (``jsonl_on_error="skip"``, the default): the record is skipped and
  counted in ``report.error_count``, and the records after it still profile;
- strict: the first such record raises a ValueError naming the position and the
  value's JSON kind, never the record contents.

Input whose every record is non-object always fails, matching the all-malformed
policy. Distinct from #463 (zero-field *objects*) and #486 (physical record
boundaries).

The async cases are skipped when the extension is built without async support.

Run after building the extension:
    maturin develop --features python,python-async,async-streaming
    pytest python/tests/test_json_record_policy.py -v
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

# Every JSON value that is not an object, in the middle of two valid records so
# a scanner that mishandles one loses the record after it too.
NON_OBJECT_VALUES = [
    ("null", b"null"),
    ("boolean", b"true"),
    ("number", b"42"),
    ("negative number", b"-1.5e3"),
    ("string", b'"text"'),
    ("array", b"[1, 2]"),
]


def _jsonl(value: bytes) -> bytes:
    return b'{"id":1}\n' + value + b'\n{"id":2}\n'


def _json_array(value: bytes) -> bytes:
    return b'[{"id":1}, ' + value + b', {"id":2}]'


def _write(tmp_path: Path, data: bytes, suffix: str) -> str:
    target = tmp_path / f"data.{suffix}"
    target.write_bytes(data)
    return str(target)


def _async_bytes(data: bytes, fmt: str, **kwargs):
    from dataprof.asyncio import profile_bytes

    async def _inner():
        return await profile_bytes(data, format=fmt, **kwargs)

    return asyncio.run(_inner())


# --- Tolerant: counted, never silently dropped -----------------------------


@pytest.mark.parametrize(
    ("kind", "value"), NON_OBJECT_VALUES, ids=[k for k, _ in NON_OBJECT_VALUES]
)
@pytest.mark.parametrize("fmt", ["jsonl", "json"])
def test_file_tolerant_counts_non_object_record(tmp_path, kind, value, fmt):
    payload = _jsonl(value) if fmt == "jsonl" else _json_array(value)
    report = dataprof.profile(_write(tmp_path, payload, fmt), format=fmt)
    assert report.rows == 2, f"{kind}: the record after it must still profile"
    assert report.error_count == 1


@pytest.mark.parametrize(
    ("kind", "value"), NON_OBJECT_VALUES, ids=[k for k, _ in NON_OBJECT_VALUES]
)
@pytest.mark.parametrize("fmt", ["jsonl", "json"])
def test_bytes_tolerant_counts_non_object_record(kind, value, fmt):
    payload = _jsonl(value) if fmt == "jsonl" else _json_array(value)
    report = dataprof.profile(payload, format=fmt)
    assert report.rows == 2, f"{kind}: the record after it must still profile"
    assert report.error_count == 1


@requires_async
@pytest.mark.parametrize(
    ("kind", "value"), NON_OBJECT_VALUES, ids=[k for k, _ in NON_OBJECT_VALUES]
)
@pytest.mark.parametrize("fmt", ["jsonl", "json"])
def test_async_bytes_tolerant_counts_non_object_record(kind, value, fmt):
    payload = _jsonl(value) if fmt == "jsonl" else _json_array(value)
    report = _async_bytes(payload, fmt)
    assert report.rows == 2, f"{kind}: the record after it must still profile"
    assert report.error_count == 1


@pytest.mark.parametrize(
    "payload", [b'2\n{"id":1}\n', b'{"id":1}\n2\n', b'{"id":1}\n{"id":2}\n2\n']
)
def test_first_middle_and_last_positions_are_all_counted(tmp_path, payload):
    """A non-object record counts wherever it sits, not only mid-stream."""
    expected_rows = payload.count(b'{"id":')
    for report in (
        dataprof.profile(_write(tmp_path, payload, "jsonl"), format="jsonl"),
        dataprof.profile(payload, format="jsonl"),
    ):
        assert report.rows == expected_rows
        assert report.error_count == 1


# --- Strict: rejected at the first non-object record -----------------------


@pytest.mark.parametrize(
    ("kind", "value"), NON_OBJECT_VALUES, ids=[k for k, _ in NON_OBJECT_VALUES]
)
@pytest.mark.parametrize("fmt", ["jsonl", "json"])
def test_file_strict_rejects_non_object_record(tmp_path, kind, value, fmt):
    payload = _jsonl(value) if fmt == "jsonl" else _json_array(value)
    path = _write(tmp_path, payload, fmt)
    with pytest.raises(ValueError, match="non-object JSON record"):
        dataprof.profile(path, format=fmt, jsonl_on_error="strict")


@pytest.mark.parametrize(
    ("kind", "value"), NON_OBJECT_VALUES, ids=[k for k, _ in NON_OBJECT_VALUES]
)
@pytest.mark.parametrize("fmt", ["jsonl", "json"])
def test_bytes_strict_rejects_non_object_record(kind, value, fmt):
    payload = _jsonl(value) if fmt == "jsonl" else _json_array(value)
    with pytest.raises(ValueError, match="non-object JSON record"):
        dataprof.profile(payload, format=fmt, jsonl_on_error="strict")


@requires_async
@pytest.mark.parametrize(
    ("kind", "value"), NON_OBJECT_VALUES, ids=[k for k, _ in NON_OBJECT_VALUES]
)
@pytest.mark.parametrize("fmt", ["jsonl", "json"])
def test_async_bytes_strict_rejects_non_object_record(kind, value, fmt):
    payload = _jsonl(value) if fmt == "jsonl" else _json_array(value)
    with pytest.raises(ValueError, match="non-object JSON record"):
        _async_bytes(payload, fmt, jsonl_on_error="strict")


def test_strict_error_names_the_kind_and_never_the_contents(tmp_path):
    secret = b'"topsecret@example.com"'
    path = _write(tmp_path, _jsonl(secret), "jsonl")
    with pytest.raises(ValueError) as excinfo:
        dataprof.profile(path, format="jsonl", jsonl_on_error="strict")
    message = str(excinfo.value)
    assert "found string" in message
    assert "topsecret" not in message


# --- Records with nothing profileable in them ------------------------------


@pytest.mark.parametrize(
    ("payload", "fmt"),
    [
        (b'"just a string"', "jsonl"),
        (b"1\n2\n3\n", "jsonl"),
        (b"[1, 2, 3]", "json"),
        (b"[null]", "json"),
    ],
)
def test_input_without_any_object_record_fails(tmp_path, payload, fmt):
    """Returning an empty-but-clean profile would fabricate a clean dataset."""
    path = _write(tmp_path, payload, fmt)
    with pytest.raises(ValueError, match="No valid JSON records|no valid JSON records"):
        dataprof.profile(path, format=fmt)
    with pytest.raises(ValueError, match="No valid JSON records|no valid JSON records"):
        dataprof.profile(payload, format=fmt)


# --- Parity with the malformed-record category ------------------------------


def test_non_object_and_malformed_records_both_count_once(tmp_path):
    """The two categories share ``error_count`` but stay separately reported."""
    payload = b'{"id":1}\n2\nnot-json\n{"id":2}\n'
    for report in (
        dataprof.profile(_write(tmp_path, payload, "jsonl"), format="jsonl"),
        dataprof.profile(payload, format="jsonl"),
    ):
        assert report.rows == 2
        assert report.error_count == 2

    with pytest.raises(ValueError, match="non-object JSON record"):
        dataprof.profile(payload, format="jsonl", jsonl_on_error="strict")


def test_a_number_element_does_not_swallow_the_array_delimiter(tmp_path):
    """serde ends a number by peeking one byte past it.

    That byte is the array's ``,``; losing it with the deserializer used to
    strand every element after the first number.
    """
    payload = b'[{"id":1}, 1, 2, 3, {"id":2}, 4, {"id":3}]'
    for report in (
        dataprof.profile(_write(tmp_path, payload, "json"), format="json"),
        dataprof.profile(payload, format="json"),
    ):
        assert report.rows == 3
        assert report.error_count == 4


def test_malformed_number_stays_a_malformed_record(tmp_path):
    """``1.2.3`` reads like a number but is not one, so it is not miscategorised."""
    path = _write(tmp_path, b'{"id":1}\n1.2.3\n', "jsonl")
    with pytest.raises(ValueError, match="malformed JSON record"):
        dataprof.profile(path, format="jsonl", jsonl_on_error="strict")
