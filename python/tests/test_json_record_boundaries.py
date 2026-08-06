"""JSONL record boundaries mean the same thing on every transport (#486).

`format="jsonl"` is **one record per physical line**: a record may not span
lines, and a line may not hold more than one value. `format="json"` is a
**standard JSON document**: one array of records, or one object as a single
record, either of which may be pretty-printed.

Before this was settled, the file and async scanners read JSONL as a
whitespace-delimited stream of JSON values while the synchronous bytes reader
split on lines. The same payload therefore had incompatible answers:
`b'{"x":1}{"x":2}'` — a concatenation with the delimiter lost — profiled as two
clean rows on one path and failed on another.

Every case below is checked on all four transports at once, because the bug was
never visible from any single one.
"""

from __future__ import annotations

import asyncio

import dataprof as dp
import pytest
from dataprof.asyncio import (
    profile_bytes as profile_bytes_async,
    profile_file as profile_file_async,
)

requires_async = pytest.mark.skipif(
    not dp.capabilities().async_streaming,
    reason="Async streaming not compiled. Build with --features "
    "'python,python-async,async-streaming'.",
)

#: Sentinel for "this input is rejected", so an error compares equal across
#: transports without depending on each one's message.
REJECTED = "rejected"

#: What one transport reported: `(rows, error_count)`, or `REJECTED`.
Outcome = str | tuple[int, int]


def _outcome(call) -> Outcome:
    """`(rows, error_count)` for a profile that succeeded, else `REJECTED`."""
    try:
        report = call()
    except ValueError:
        return REJECTED
    return (report.rows, report.error_count)


def _all_transports(payload: bytes, fmt: str, tmp_path, **kwargs) -> dict[str, Outcome]:
    path = tmp_path / f"fixture.{fmt}"
    path.write_bytes(payload)
    transports = {
        "file": lambda: dp.profile(path, **kwargs),
        "bytes": lambda: dp.profile(payload, format=fmt, **kwargs),
    }
    if dp.capabilities().async_streaming:
        transports["async file"] = lambda: asyncio.run(profile_file_async(path, **kwargs))
        transports["async bytes"] = lambda: asyncio.run(
            profile_bytes_async(payload, format=fmt, **kwargs)
        )
    return {name: _outcome(call) for name, call in transports.items()}


def _agree(results: dict[str, Outcome]) -> Outcome:
    """Assert every transport reached the same answer, and return it."""
    distinct = set(results.values())
    assert len(distinct) == 1, f"transports disagree: {results}"
    return next(iter(distinct))


# ---------------------------------------------------------------------------
# JSONL: one record per physical line
# ---------------------------------------------------------------------------

JSONL_CASES = [
    # (label, payload, expected)
    # A concatenation with the delimiter lost. Reading it as a value stream
    # produced two clean rows and hid the damage; it is now rejected.
    ("adjacent values", b'{"x":1}{"x":2}', REJECTED),
    # The same fault followed by a good record: the bad line is counted, the
    # good one still profiles.
    ("adjacent then valid", b'{"x":1}{"x":2}\n{"x":3}\n', (1, 1)),
    ("space separated on one line", b'{"x":1} {"x":2}\n', REJECTED),
    # A record that spans lines is not JSONL, however valid the JSON is.
    ("pretty-printed object", b'{\n  "x": 1\n}\n', REJECTED),
    ("clean records", b'{"x":1}\n{"x":2}\n', (2, 0)),
    # Blank lines are separators, not records: no rows and no errors.
    ("blank lines between records", b'{"x":1}\n\n\n{"x":2}\n', (2, 0)),
    ("malformed first record", b'nope\n{"x":1}\n', (1, 1)),
    ("malformed middle record", b'{"x":1}\nnope\n{"x":2}\n', (2, 1)),
    ("malformed last record", b'{"x":1}\nnope\n', (1, 1)),
    ("truncated last record", b'{"x":1}\n{"x":2', (1, 1)),
    # A record with no fields is still a record (#533).
    ("fieldless record", b'{}\n{"x":1}\n', (2, 0)),
    ("no trailing newline", b'{"x":1}\n{"x":2}', (2, 0)),
]


@pytest.mark.parametrize(
    ("label", "payload", "expected"),
    JSONL_CASES,
    ids=[case[0] for case in JSONL_CASES],
)
def test_jsonl_record_boundaries_agree_on_every_transport(label, payload, expected, tmp_path):
    results = _all_transports(payload, "jsonl", tmp_path)
    assert _agree(results) == expected, f"{label}: {results}"


# ---------------------------------------------------------------------------
# JSON: one document, array or single object
# ---------------------------------------------------------------------------

JSON_CASES = [
    ("array of records", b'[{"x":1},{"x":2}]', (2, 0)),
    # Whitespace is insignificant inside a JSON document, so pretty-printing
    # changes nothing — including the error count, which used to gain a phantom
    # malformed record from the newline before `]`.
    ("pretty-printed array", b'[\n  {"x": 1},\n  {"x": 2}\n]\n', (2, 0)),
    ("single object", b'{"type":"FC","n":2}', (1, 0)),
    ("pretty-printed single object", b'{\n  "type": "FC",\n  "n": 2\n}\n', (1, 0)),
    ("fieldless object", b"{}", (1, 0)),
    # This is what a JSONL file looks like when read with the JSON grammar.
    ("concatenated objects", b'{"x":1}{"x":2}', REJECTED),
]


@pytest.mark.parametrize(
    ("label", "payload", "expected"),
    JSON_CASES,
    ids=[case[0] for case in JSON_CASES],
)
def test_json_document_shapes_agree_on_every_transport(label, payload, expected, tmp_path):
    results = _all_transports(payload, "json", tmp_path)
    assert _agree(results) == expected, f"{label}: {results}"


# ---------------------------------------------------------------------------
# The two grammars are distinct, and strict mode rejects rather than counts
# ---------------------------------------------------------------------------


def test_the_same_payload_reads_differently_under_each_grammar(tmp_path):
    # A pretty-printed object is one record as JSON and not JSONL at all. That
    # is the whole point of having two grammars rather than one lenient reader.
    payload = b'{\n  "x": 1,\n  "y": 2\n}\n'

    assert _agree(_all_transports(payload, "json", tmp_path)) == (1, 0)
    assert _agree(_all_transports(payload, "jsonl", tmp_path)) is REJECTED


@pytest.mark.parametrize(
    ("label", "payload"),
    [
        ("adjacent values", b'{"x":1}{"x":2}\n{"x":3}\n'),
        ("malformed record", b'{"x":1}\nnope\n'),
    ],
)
def test_strict_mode_rejects_what_skip_mode_counts(label, payload, tmp_path):
    skipped = _agree(_all_transports(payload, "jsonl", tmp_path))
    assert isinstance(skipped, tuple), f"{label}: skip mode should have counted the fault"
    assert skipped[1] > 0, f"{label}: skip mode must report the fault, got {skipped}"

    strict = _all_transports(payload, "jsonl", tmp_path, jsonl_on_error="strict")
    assert _agree(strict) is REJECTED, f"{label}: {strict}"


def test_a_malformed_record_is_located_by_its_physical_line(tmp_path):
    # Each line is parsed on its own, so the decoder's own line number is always
    # 1; the diagnostic must name the line in the file instead.
    payload = b'{"x":1}\n\n{"x":2}\nnope\n'
    path = tmp_path / "fixture.jsonl"
    path.write_bytes(payload)

    with pytest.raises(ValueError) as excinfo:
        dp.profile(path, jsonl_on_error="strict")
    assert "line 4" in str(excinfo.value), str(excinfo.value)


def test_the_json_grammar_error_points_at_the_jsonl_option(tmp_path):
    # Reading a JSONL file as JSON is the likely mistake, so the error names the
    # option that reads it correctly.
    path = tmp_path / "fixture.json"
    path.write_bytes(b'{"x":1}\n{"x":2}\n')

    with pytest.raises(ValueError) as excinfo:
        dp.profile(path)
    assert "jsonl" in str(excinfo.value).lower(), str(excinfo.value)


@requires_async
def test_async_and_sync_agree_on_a_large_jsonl_stream(tmp_path):
    # Chunk boundaries are where a line-based reader would most plausibly split a
    # record; profile enough rows to cross several.
    payload = b"".join(b'{"id":%d,"name":"row-%d"}\n' % (i, i) for i in range(5_000))
    results = _all_transports(payload, "jsonl", tmp_path)
    assert _agree(results) == (5_000, 0)
