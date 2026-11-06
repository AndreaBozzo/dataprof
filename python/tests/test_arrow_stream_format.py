"""An Arrow C stream records ``format: arrow`` and declares its columns (#1230).

The stream's schema lists every column before the first batch, so the baseline
gate reads a capped Arrow stream as it reads a capped CSV or Parquet file: a
missing required column fails instead of staying ``evidence_incomplete``. A
stored report keeps that reading, and one without ``format`` (written before
0.14) keeps the conservative one.

``tests/arrow_stream_format.rs`` is the Rust twin; it covers the stored-report
reading and the routes that must refuse an ``arrow`` format override.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

try:
    import dataprof as dp
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

pa = pytest.importorskip("pyarrow")


class _Producer:
    """An ``__arrow_c_stream__`` object that is not a pyarrow type."""

    def __init__(self, reader: Any) -> None:
        self.reader = reader

    def __arrow_c_stream__(self, requested_schema: object = None) -> object:
        return self.reader.__arrow_c_stream__(requested_schema)


def _reader() -> Any:
    table = pa.table({"id": [1, 2, 3], "amount": [10, 20, 30]})
    return pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=1))


def _stream_routes(max_rows: int | None) -> dict[str, dp.ProfileReport]:
    return {
        "RecordBatchReader": dp.profile(_reader(), max_rows=max_rows),
        "custom producer": dp.profile(_Producer(_reader()), max_rows=max_rows),
    }


def _note_outcome(report: dp.ProfileReport) -> str:
    (check,) = report.check(require_columns=["note"]).checks
    if check.status != "not_evaluated":
        return check.status
    assert check.reason is not None
    return f"{check.reason['reason']}:{check.reason.get('gap')}"


def _document(report: dp.ProfileReport) -> dict:
    return json.loads(report.to_json())


def _stored(document: dict) -> dp.ProfileReport:
    return dp.ProfileReport.from_dict(document)


def test_a_capped_arrow_stream_declares_its_columns() -> None:
    wrong = []
    for max_rows in (1, None):
        for route, report in _stream_routes(max_rows).items():
            source = _document(report)["data_source"]
            if source.get("format") != "arrow":
                wrong.append((route, max_rows, "format", source.get("format")))
            outcome = _note_outcome(report)
            if outcome != "failed":
                wrong.append((route, max_rows, "require note", outcome))
            stored = _note_outcome(_stored(_document(report)))
            if stored != "failed":
                wrong.append((route, max_rows, "stored require note", stored))
    assert wrong == []


def test_a_stored_stream_without_format_stays_conservative() -> None:
    document = _document(dp.profile(_reader(), max_rows=1))
    del document["data_source"]["format"]
    assert _note_outcome(_stored(document)) == "evidence_incomplete:truncated"


def test_the_arrow_format_is_not_a_file_override(tmp_path) -> None:
    path = tmp_path / "data.csv"
    path.write_text("id,amount\n1,10\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Unknown format 'arrow'"):
        dp.profile(str(path), format="arrow")
