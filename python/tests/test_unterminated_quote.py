"""A CSV quote that is never closed must not read as a clean file (#782).

The CSV parsers accept end of input inside a quoted field as the end of that
field, so an unclosed quote swallows every following row into one value.
``report.unterminated_quote`` is the sign of it on the paths that recover, and
the Rust twin is ``tests/unterminated_quote.rs``:

====================  =========================================================
file (every engine)   profile, ``unterminated_quote`` is ``True``
async bytes / file    profile, ``unterminated_quote`` is ``True``
sync bytes            reject with a ValueError naming the line, as it rejects a
                      ragged row
``csv_flexible``      ``False`` rejects on the file and async paths
====================  =========================================================

``None`` means the check did not run: non-CSV input, or a scan that stopped
before the end of its source, where that quote sits.
"""

from __future__ import annotations

import asyncio
import csv
import io
import itertools
from pathlib import Path

import dataprof as dp
import pytest
from dataprof._inputs import _unclosed_quote_at

UNCLOSED = b'id,text\n1,"never closed\n2,x\n3,y\n'
CLOSED_ACROSS_LINES = b'id,text\n1,"line one\nline two"\n2,x\n'
ENGINES = ("auto", "incremental", "columnar")

requires_async = pytest.mark.skipif(
    not dp.capabilities().async_streaming,
    reason="Async streaming not compiled. Build with --features "
    "'python,python-async,async-streaming'.",
)


def _write(tmp_path: Path, data: bytes, name: str = "data.csv") -> Path:
    target = tmp_path / name
    target.write_bytes(data)
    return target


def _async_bytes(data: bytes, **kwargs):
    from dataprof.asyncio import profile_bytes

    return asyncio.run(profile_bytes(data, format="csv", **kwargs))


@pytest.mark.parametrize("engine", ENGINES)
def test_an_unclosed_quote_is_reported(tmp_path, engine):
    report = dp.profile_file(_write(tmp_path, UNCLOSED), engine=engine)
    assert report.rows == 1
    assert report.unterminated_quote is True
    assert [f.code for f in report.findings()].count("unterminated_quote") == 1


@pytest.mark.parametrize("engine", ENGINES)
def test_a_closed_quote_across_lines_is_clean(tmp_path, engine):
    report = dp.profile_file(_write(tmp_path, CLOSED_ACROSS_LINES), engine=engine)
    assert report.rows == 2
    assert report.unterminated_quote is False
    assert "unterminated_quote" not in [f.code for f in report.findings()]


@pytest.mark.parametrize("engine", ENGINES)
def test_strict_parsing_refuses(tmp_path, engine):
    with pytest.raises(ValueError, match="ends inside a quoted field"):
        dp.profile_file(_write(tmp_path, UNCLOSED), engine=engine, csv_flexible=False)


@pytest.mark.parametrize("engine", ENGINES)
def test_a_scan_stopped_before_the_end_does_not_answer(tmp_path, engine):
    path = _write(tmp_path, b'id,text\n1,a\n2,b\n3,"open\n4,x\n')
    report = dp.profile_file(path, engine=engine, stop_condition=dp.StopCondition.max_rows(1))
    assert not report.source_exhausted
    assert report.unterminated_quote is None


def test_json_input_is_not_checked(tmp_path):
    path = _write(tmp_path, b'{"text": "a \\"quoted\\" value"}\n', "data.jsonl")
    assert dp.profile_file(path).unterminated_quote is None


def test_sync_bytes_refuse_and_name_the_line():
    with pytest.raises(ValueError, match="opened on line 2 is never closed"):
        dp.profile(UNCLOSED, format="csv")
    assert dp.profile(CLOSED_ACROSS_LINES, format="csv").rows == 2


@requires_async
def test_async_bytes_report_the_same_as_the_file():
    report = _async_bytes(UNCLOSED)
    assert report.rows == 1
    assert report.unterminated_quote is True
    assert _async_bytes(CLOSED_ACROSS_LINES).unterminated_quote is False
    with pytest.raises(ValueError, match="ends inside a quoted field"):
        _async_bytes(UNCLOSED, csv_flexible=False)


def test_a_saved_report_keeps_the_flag_and_the_finding(tmp_path):
    report = dp.profile_file(_write(tmp_path, UNCLOSED))
    document = report.to_dict()
    assert document["execution"]["unterminated_quote"] is True
    loaded = dp.ProfileReport.from_dict(document)
    assert loaded.unterminated_quote is True
    assert "unterminated_quote" in [f.code for f in loaded.findings()]
    assert loaded.to_dict()["execution"]["unterminated_quote"] is True

    # An unchecked report omits the field and reads back as unchecked.
    json_path = _write(tmp_path, b'{"a": 1}\n', "data.jsonl")
    document = dp.profile_file(json_path).to_dict()
    assert "unterminated_quote" not in document["execution"]
    assert dp.ProfileReport.from_dict(document).unterminated_quote is None


def _rows(text: str) -> int:
    return sum(1 for _ in csv.reader(io.StringIO(text, newline="")))


def test_the_quote_scan_agrees_with_the_csv_module_on_every_short_input():
    # Text appended after a line break starts a new row, unless the input
    # ended inside a quoted field, where it lands in that field instead.
    inside = 0
    for length in range(7):
        for chars in itertools.product('a,"\r\n', repeat=length):
            text = "".join(chars)
            expected = _rows(text + "\nZ") == _rows(text)
            assert (_unclosed_quote_at(text, ",") is not None) == expected, repr(text)
            inside += expected
    assert inside > 2_000
