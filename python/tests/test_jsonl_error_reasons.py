"""A strict JSONL error names the decoder's reason on every route (#1143).

The file and async readers gave every decode fault the record-boundary text,
and the bytes route gave a position with no reason. Each route now gives its
decoder's reason, and the boundary rule only for data after a complete value
and a value the line break cuts short. The column counts characters of the
source line, leading white space included, and a record that ends early is
located just past its last character on every route.

The bytes route locates a token its ``json.loads`` hooks refuse by searching
for it, and found the first occurrence of the text, even inside a string. It
now finds the token itself.

``tests/jsonl_error_reasons.rs`` is the Rust twin for the file and async routes.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import dataprof
import pytest

_HAS_ASYNC = dataprof.capabilities().async_streaming

BOUNDARY = "a JSONL record must be one complete JSON value on one line"

#: (label, line 2, file and async tail, bytes tail), where a tail is what the
#: error says after "malformed JSON record on line 2, ".
CASES = [
    ("number out of range", b'{"v":1e400}', "column 10: number out of range",
     "column 6 (a number outside the 64-bit float range)."),
    ("invalid escape", b'{"v":"a\\qb"}', "column 9: invalid escape",
     "column 8 (Invalid \\escape)."),
    ("control character", b'{"v":"a\x01b"}',
     "column 8: control character (\\u0000-\\u001F) found while parsing a string",
     "column 8 (Invalid control character)."),
    ("missing comma", b'{"v":1 "w":2}', "column 8: expected `,` or `}`",
     "column 8 (Expecting ',' delimiter)."),
    ("trailing value", b'{"v":1} {"v":2}', f"column 9: trailing characters; {BOUNDARY}",
     f"column 9 (Extra data; {BOUNDARY})."),
    ("cut short", b'{"v":1', f"column 7: EOF while parsing an object; {BOUNDARY}",
     f"column 7 (Expecting ',' delimiter; {BOUNDARY})."),
    ("cut short after white space", b'{"x":   ', f"column 6: EOF while parsing a value; {BOUNDARY}",
     f"column 6 (Expecting value; {BOUNDARY})."),
    ("unterminated string", b'{"x":"ab', f"column 9: EOF while parsing a string; {BOUNDARY}",
     f"column 9 (Unterminated string; {BOUNDARY})."),
    # json places these at the cut token's start, or past the part it read.
    ("cut-short literal", b'{"v":tru', f"column 9: EOF while parsing a value; {BOUNDARY}",
     f"column 9 (Expecting value; {BOUNDARY})."),
    ("cut-short exponent", b'{"v":1e', f"column 8: EOF while parsing a value; {BOUNDARY}",
     f"column 8 (Expecting ',' delimiter; {BOUNDARY})."),
    ("cut-short top-level number", b"1e", f"column 3: EOF while parsing a value; {BOUNDARY}",
     f"column 3 (Extra data; {BOUNDARY})."),
    ("cut-short escape", b'{"v":"\\u12', f"column 11: EOF while parsing a string; {BOUNDARY}",
     f"column 11 (Invalid \\uXXXX escape; {BOUNDARY})."),
    # json reads 0 and then data after it; serde refuses the number.
    ("leading zero", b"01", "column 2: invalid number", "column 2 (Extra data)."),
    ("zero, then a non-ASCII digit", "0²".encode(), f"column 2: trailing characters; {BOUNDARY}",
     f"column 2 (Extra data; {BOUNDARY})."),
    ("leading white space", b"  {bad}", "column 4: key must be a string",
     "column 4 (Expecting property name enclosed in double quotes)."),
    ("non-ASCII before the fault", '{"éé":1 "w":2}'.encode(), "column 9: expected `,` or `}`",
     "column 9 (Expecting ',' delimiter)."),
]  # fmt: skip

#: Bytes only: a refused token whose text also appears earlier in the line.
#: (label, line 2, bytes tail)
TOKEN_CASES = [
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
    for i, (label, line, rust_tail, bytes_tail) in enumerate(CASES):
        data = _data(line)
        path = tmp_path / f"case{i}.jsonl"
        path.write_bytes(data)
        routes = {
            "file": (
                lambda: dataprof.profile(str(path), format="jsonl", jsonl_on_error="strict"),
                f"malformed JSON record on line 2, {rust_tail}",
            ),
            "bytes": (
                lambda: dataprof.profile(data, format="jsonl", jsonl_on_error="strict"),
                f"jsonl bytes: malformed JSON record on line 2, {bytes_tail}",
            ),
        }
        if _HAS_ASYNC:
            routes["async"] = (
                lambda: _async_bytes(data),
                f"malformed JSON record on line 2, {rust_tail}",
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
