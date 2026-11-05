"""A strict JSONL error names the decoder's reason on every route (#1143).

The file and async readers gave every decode fault the record-boundary text,
and the bytes route gave a position with no reason. serde's reason is now
given, and the boundary rule only for data after a complete value and a value
the line break cuts short. The column counts characters of the source line,
leading white space included, and a record that ends early is located just
past its last character. The bytes route decodes with ``json`` and takes the
file readers' message for a line ``json`` refuses, so all three routes report
the same text.

A token the bytes route's ``json.loads`` hooks refuse keeps the bytes route's
own description. It was located by searching for its text, which found the
first occurrence, even inside a string; it is now found where ``json`` read it.

``tests/jsonl_error_reasons.rs`` is the Rust twin for the file and async routes.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import dataprof
import pytest

_HAS_ASYNC = dataprof.capabilities().async_streaming

BOUNDARY = "a JSONL record must be one complete JSON value on one line"

#: (label, line 2, tail), where the tail is what every route says after
#: "malformed JSON record on line 2, "; bytes adds its prefix and a full stop.
CASES = [
    ("invalid escape", b'{"v":"a\\qb"}', "column 9: invalid escape"),
    ("control character", b'{"v":"a\x01b"}',
     "column 8: control character (\\u0000-\\u001F) found while parsing a string"),
    ("missing comma", b'{"v":1 "w":2}', "column 8: expected `,` or `}`"),
    ("trailing value", b'{"v":1} {"v":2}', f"column 9: trailing characters; {BOUNDARY}"),
    ("cut short", b'{"v":1', f"column 7: EOF while parsing an object; {BOUNDARY}"),
    ("cut short after white space", b'{"x":   ',
     f"column 6: EOF while parsing a value; {BOUNDARY}"),
    ("unterminated string", b'{"x":"ab', f"column 9: EOF while parsing a string; {BOUNDARY}"),
    # json places these at the cut token's start, or past the part it read.
    ("cut-short literal", b'{"v":tru', f"column 9: EOF while parsing a value; {BOUNDARY}"),
    ("cut-short exponent", b'{"v":1e', f"column 8: EOF while parsing a value; {BOUNDARY}"),
    ("cut-short top-level number", b"1e", f"column 3: EOF while parsing a value; {BOUNDARY}"),
    ("cut-short escape", b'{"v":"\\u12', f"column 11: EOF while parsing a string; {BOUNDARY}"),
    # Trailing data that is itself a cut-short literal is still trailing data.
    ("trailing cut-short number", b'{"a":1} 1e', f"column 9: trailing characters; {BOUNDARY}"),
    ("trailing cut-short literal", b'{"a":1}tru', f"column 8: trailing characters; {BOUNDARY}"),
    # A cut-short-looking key or value after a complete one is a syntax error.
    ("literal for a key", b"{tru", "column 2: key must be a string"),
    ("literal after a value", b'{"v":1 tru', "column 8: expected `,` or `}`"),
    # json reads a number's valid start and then data after it; serde refuses
    # the number.
    ("leading zero", b"01", "column 2: invalid number"),
    ("fraction without digits", b"1.e5", "column 3: invalid number"),
    ("exponent without digits", b"1eX", "column 3: invalid number"),
    ("second exponent", b"1e2e3", f"column 4: trailing characters; {BOUNDARY}"),
    ("zero, then a non-ASCII digit", "0²".encode(), f"column 2: trailing characters; {BOUNDARY}"),
    ("leading white space", b"  {bad}", "column 4: key must be a string"),
    ("non-ASCII before the fault", '{"éé":1 "w":2}'.encode(), "column 9: expected `,` or `}`"),
]  # fmt: skip

#: Bytes only: a refused token whose text also appears earlier in the line.
#: (label, line 2, bytes tail)
TOKEN_CASES = [
    ("number out of range", b'{"v":1e400}',
     "column 6 (a number outside the 64-bit float range)."),
    ("float in a string first", b'{"a":"1e400","v":1e400}',
     "column 18 (a number outside the 64-bit float range)."),
    ("integer in a string first", b'{"a":"18446744073709551616","v":18446744073709551616}',
     "column 33 (an integer outside the 64-bit range; "
     "write such integers as strings to profile them)."),
    ("constant in a string first", b'{"a":"NaN","v":NaN}',
     "column 16 (non-standard numeric constant 'NaN')."),
    # json hands the number to its hook before it reads the text after it.
    ("refused number before other text", b'{"v":1e400foo,"w":1e400}',
     "column 6 (a number outside the 64-bit float range)."),
    # An accepted float whose digits spell the refused integer.
    ("integer inside an earlier literal",
     b'{"a":18446744073709551616e-10,"v":18446744073709551616}',
     "column 35 (an integer outside the 64-bit range; "
     "write such integers as strings to profile them)."),
    ("surrogate pair before a lone surrogate", b'{"a":"\\ud83d\\ude00","v":"\\ud83d"}',
     "column 26 (a lone surrogate escape, which is not valid Unicode)."),
    ("low surrogate after a joined pair", b'{"v":"\\ud83d\\ude00\\ude00"}',
     "column 19 (a lone surrogate escape, which is not valid Unicode)."),
    # `\\` is one escape, so the text after it only looks like a high escape.
    ("low surrogate after an escaped backslash", b'{"v":"\\\\ud800\\udc00"}',
     "column 14 (a lone surrogate escape, which is not valid Unicode)."),
]  # fmt: skip


def _data(line: bytes) -> bytes:
    return b'{"v":0}\n' + line + b'\n{"v":2}\n'


def _matches(error: str, expected: str) -> bool:
    """Whether ``error`` carries exactly the expected message, so a reason with
    the boundary rule appended does not pass for one without it."""
    at = error.find(expected)
    return at >= 0 and not error[at + len(expected) :].startswith(";")


def _strict_error(run) -> str:
    try:
        run()
    except ValueError as error:
        return str(error)
    return "profiled instead of refusing"


def _async_bytes(data: bytes):
    from dataprof.asyncio import profile_bytes

    return asyncio.run(profile_bytes(data, format="jsonl", jsonl_on_error="strict"))


def test_every_route_names_the_decoders_reason(tmp_path: Path):
    """Collects every wrong (case, route) before failing."""
    wrong = []
    for i, (label, line, tail) in enumerate(CASES):
        data = _data(line)
        path = tmp_path / f"case{i}.jsonl"
        path.write_bytes(data)
        routes = {
            "file": (
                lambda: dataprof.profile(str(path), format="jsonl", jsonl_on_error="strict"),
                f"malformed JSON record on line 2, {tail}",
            ),
            "bytes": (
                lambda: dataprof.profile(data, format="jsonl", jsonl_on_error="strict"),
                f"jsonl bytes: malformed JSON record on line 2, {tail}.",
            ),
        }
        if _HAS_ASYNC:
            routes["async"] = (
                lambda: _async_bytes(data),
                f"malformed JSON record on line 2, {tail}",
            )
        for route, (run, expected) in routes.items():
            error = _strict_error(run)
            if not _matches(error, expected):
                wrong.append(f"[{label}] {route}: {error}")
    assert not wrong, "wrong strict JSONL errors:\n" + "\n".join(wrong)


def test_bytes_route_locates_the_refused_token():
    wrong = []
    for label, line, tail in TOKEN_CASES:
        expected = f"jsonl bytes: malformed JSON record on line 2, {tail}"
        error = _strict_error(
            lambda: dataprof.profile(_data(line), format="jsonl", jsonl_on_error="strict")
        )
        if not _matches(error, expected):
            wrong.append(f"[{label}] bytes: {error}")
    assert not wrong, "wrong strict JSONL errors:\n" + "\n".join(wrong)


@pytest.mark.parametrize("line", [b'{"v":1e400}', b'{"v":"a\\qb"}', b'{"v":1} {"v":2}'])
def test_skip_policy_still_counts_the_record(line: bytes):
    report = dataprof.profile(_data(line), format="jsonl")
    assert report.rows == 2
    assert report.error_count == 1
