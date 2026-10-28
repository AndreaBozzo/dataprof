"""A lone surrogate escape in JSON is refused, not passed on to crash.

``json.loads`` keeps an unpaired ``\\ud800`` escape as a code point no UTF-8
string can hold, so synchronous bytes raised ``UnicodeEncodeError`` when the
record reached the core, under ``skip`` as well as ``strict``, while the file
and async readers refused the record while decoding (#1142). Bytes now refuse
it as a decode error: strict raises, skip drops and counts a JSONL record, and
a JSON document, which bytes load at once, is refused whole.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

try:
    import dataprof as dp
    from dataprof.asyncio import profile_bytes, profile_file
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

LONE = "a lone surrogate escape"

#: label -> the middle record, as JSON text: the backslashes are the file's.
LONE_RECORDS: dict[str, str] = {
    "high": r'{"v":"\ud800"}',
    "low": r'{"v":"x\udc00y"}',
    "upper-case hex": r'{"v":"\uD800"}',
    "in a key": r'{"\ud800":1}',
    "nested in an array": r'{"v":{"k":["\ud800"]}}',
    "high before a non-surrogate": r'{"v":"\ud800A"}',
    "two highs": r'{"v":"\ud800\ud800"}',
    "low before high": r'{"v":"\udc00\ud800"}',
}

#: Escapes every route accepts: a pair is one character, and an escaped
#: backslash makes the rest literal text.
ACCEPTED_RECORDS: dict[str, str] = {
    "pair": r'{"v":"😀"}',
    "upper-case pair": r'{"v":"😀"}',
    "escaped backslash": r'{"v":"\\ud800"}',
    "BMP escape": r'{"v":"é"}',
}


def _payload(fmt: str, record: str) -> bytes:
    if fmt == "jsonl":
        return ('{"v":"a"}\n' + record + '\n{"v":"b"}\n').encode()
    return ('[{"v":"a"},' + record + ',{"v":"b"}]').encode()


def _routes(path: Path, data: bytes, fmt: str, on_error: str) -> dict[str, object]:
    """Each route's (rows, error_count), or the error message it raised."""

    def attempt(call):
        try:
            report = call()
        except ValueError as exc:
            return str(exc)
        return (report.rows, report.to_dict()["execution"]["error_count"])

    return {
        "file": attempt(lambda: dp.profile(path, jsonl_on_error=on_error)),
        "bytes": attempt(lambda: dp.profile(data, format=fmt, jsonl_on_error=on_error)),
        "async file": attempt(lambda: asyncio.run(profile_file(path, jsonl_on_error=on_error))),
        "async bytes": attempt(
            lambda: asyncio.run(profile_bytes(data, format=fmt, jsonl_on_error=on_error))
        ),
    }


def test_a_lone_surrogate_is_refused_on_every_route(tmp_path: Path) -> None:
    wrong = {}
    for label, record in LONE_RECORDS.items():
        for fmt in ("jsonl", "json"):
            data = _payload(fmt, record)
            path = tmp_path / f"probe.{fmt}"
            path.write_bytes(data)
            case = f"[{label}, {fmt}]"

            strict = _routes(path, data, fmt, "strict")
            for route, outcome in strict.items():
                if not isinstance(outcome, str):
                    wrong[f"{case} strict {route}"] = outcome
            if LONE not in str(strict["bytes"]):
                wrong[f"{case} strict bytes message"] = strict["bytes"]

            skip = _routes(path, data, fmt, "skip")
            if fmt == "json":
                # Bytes load the whole document, so they refuse it. The other
                # routes stop at the element and leave the rest unread, as at
                # any decoder error inside an array (#1106): one row, one error.
                if not (isinstance(skip["bytes"], str) and LONE in skip["bytes"]):
                    wrong[f"{case} skip bytes"] = skip["bytes"]
                for route in ("file", "async file", "async bytes"):
                    if skip[route] != (1, 1):
                        wrong[f"{case} skip {route}"] = skip[route]
            else:
                for route, outcome in skip.items():
                    if outcome != (2, 1):
                        wrong[f"{case} skip {route}"] = outcome
    assert not wrong, wrong


def test_the_strict_jsonl_error_locates_the_escape() -> None:
    data = _payload("jsonl", r'{"v":"\ud800"}')
    with pytest.raises(ValueError, match=r"line 2, column 7 \(a lone surrogate escape"):
        dp.profile(data, format="jsonl", jsonl_on_error="strict")


def test_paired_and_escaped_text_stays_accepted(tmp_path: Path) -> None:
    wrong = {}
    for label, record in ACCEPTED_RECORDS.items():
        for fmt in ("jsonl", "json"):
            data = _payload(fmt, record)
            path = tmp_path / f"probe.{fmt}"
            path.write_bytes(data)
            for on_error in ("strict", "skip"):
                for route, outcome in _routes(path, data, fmt, on_error).items():
                    if outcome != (3, 0):
                        wrong[f"[{label}, {fmt}] {on_error} {route}"] = outcome
    assert not wrong, wrong
