"""Unicode White_Space around a JSON record reads on every route (#1146).

A single object and a JSONL line were already read with Rust's ``str::trim`` on
the file and async routes. The array branch skipped only ASCII whitespace
before ``[`` and allowed only JSON's four after ``]``, and synchronous bytes
used ``json.loads``, which allows only those four around a document. Characters
outside the White_Space set (U+001C to U+001F) stay refused around a document.

``tests/json_unicode_whitespace.rs`` is the Rust twin.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
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

#: The Unicode White_Space characters outside JSON's own space, tab, LF, CR.
WHITE_SPACE = [
    "\x0b",
    "\x0c",
    "\x85",
    "\xa0",
    " ",
    *map(chr, range(0x2000, 0x200B)),
    " ",
    " ",
    " ",
    " ",
    "　",
]

#: Control characters outside the set, which ``str::trim`` keeps.
NOT_WHITE_SPACE = ["\x1c", "\x1f"]


def _shapes(c: str) -> list[tuple[str, str, str, str, int]]:
    """``(shape, format, text before, text after, rows)``."""
    return [
        ("object", "json", c + '{"a": 1}', '{"a": 1}' + c, 1),
        ("array", "json", c + '[{"a": 1}]', '[{"a": 1}]' + c, 1),
        ("jsonl", "jsonl", c + '{"a": 1}\n{"a": 2}\n', '{"a": 1}' + c + '\n{"a": 2}\n', 2),
    ]


def _routes(path: Path, data: bytes, fmt: str) -> dict[str, Callable[[], dp.ProfileReport]]:
    routes: dict[str, Callable[[], dp.ProfileReport]] = {
        "file": lambda: dp.profile(path, jsonl_on_error="strict"),
        "bytes": lambda: dp.profile(data, format=fmt, jsonl_on_error="strict"),
    }
    if dp.capabilities().async_streaming:
        routes["async file"] = lambda: asyncio.run(profile_file(path, jsonl_on_error="strict"))
        routes["async bytes"] = lambda: asyncio.run(
            profile_bytes(data, format=fmt, jsonl_on_error="strict")
        )
    return routes


def test_unicode_white_space_around_a_record_reads_on_every_route(tmp_path: Path) -> None:
    wrong = []
    for c in WHITE_SPACE:
        for shape, fmt, before, after, rows in _shapes(c):
            for side, text in (("before", before), ("after", after)):
                data = text.encode("utf-8")
                path = tmp_path / f"source.{fmt}"
                path.write_bytes(data)
                for route, run in _routes(path, data, fmt).items():
                    try:
                        read = run().rows
                    except (RuntimeError, ValueError) as error:
                        wrong.append(f"{route}, {shape}, U+{ord(c):04X} {side}: {error}")
                        continue
                    if read != rows:
                        wrong.append(f"{route}, {shape}, U+{ord(c):04X} {side}: {read} rows")
    assert not wrong, "\n".join(wrong)


def test_characters_outside_white_space_stay_refused_around_a_document(
    tmp_path: Path,
) -> None:
    wrong = []
    for c in NOT_WHITE_SPACE:
        for shape, fmt, before, after, _ in _shapes(c):
            if shape == "jsonl":
                continue
            for side, text in (("before", before), ("after", after)):
                data = text.encode("utf-8")
                path = tmp_path / f"source.{fmt}"
                path.write_bytes(data)
                for route, run in _routes(path, data, fmt).items():
                    try:
                        run()
                    except (RuntimeError, ValueError):
                        continue
                    wrong.append(f"{route}, {shape}, U+{ord(c):04X} {side}: read")
    assert not wrong, "\n".join(wrong)


def test_a_bytes_decode_error_keeps_its_position() -> None:
    # The white space is blanked, not cut, so the line and column still name
    # the original text.
    with pytest.raises(ValueError, match=r"line 2, column 7"):
        dp.profile('\xa0\n{"a": }'.encode(), format="json")


def test_white_space_after_a_malformed_array_is_not_unread_input(tmp_path: Path) -> None:
    # After a fault the scanners ask whether input is left unread, which sets
    # source_exhausted. A no-break space there is no more input than a space.
    wrong = []
    for c in (" ", "\xa0", "\u3000"):
        data = ('[{"a": 1}' + c).encode("utf-8")
        path = tmp_path / "source.json"
        path.write_bytes(data)
        # Synchronous bytes decode the document whole and refuse a truncated
        # array, so they have no read to leave unfinished.
        routes = {"file": lambda: dp.profile(path)}
        if dp.capabilities().async_streaming:
            routes["async bytes"] = lambda: asyncio.run(profile_bytes(data, format="json"))
        for route, run in routes.items():
            report = run()
            seen = (report.rows, report.source_exhausted, report.error_count)
            if seen != (1, True, 1):
                wrong.append(f"{route}, U+{ord(c):04X}: {seen}")
    assert not wrong, "\n".join(wrong)
