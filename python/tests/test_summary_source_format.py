"""A flat ``to_dict()`` summary records the source's format (#1028).

A summary baseline of a capped CSV, Parquet or Arrow stream run then shows its
source listed every column, as the canonical document does, so an added column
fails instead of staying ``evidence_incomplete``. A JSONL summary, and one
written before the field, still cover only the rows read. The format survives
``from_dict()`` and ``to_dict()``, which is the document the gate reads for a
loaded summary.

``tests/summary_source_format.rs`` is the Rust twin; it also covers the async
routes and the formats that declare nothing for their kind.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
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
pq = pytest.importorskip("pyarrow.parquet")

ORDERS = {"id": [1, 2, 3], "amount": [10, 20, 30], "note": ["a", "b", "c"]}
NO_NOTE = {"id": [1, 2, 3], "amount": [10, 20, 30]}


def _csv(columns: dict[str, list[Any]]) -> bytes:
    names = list(columns)
    rows = zip(*columns.values(), strict=True)
    lines = [",".join(names), *(",".join(str(value) for value in row) for row in rows)]
    return ("\n".join(lines) + "\n").encode()


def _jsonl(columns: dict[str, list[Any]]) -> bytes:
    import json

    rows = [dict(zip(columns, row, strict=True)) for row in zip(*columns.values(), strict=True)]
    if "note" not in columns:
        # The last record carries the column the capped scan never reads.
        rows[-1]["note"] = "a"
    return "".join(json.dumps(row) + "\n" for row in rows).encode()


def _reader(columns: dict[str, list[Any]]) -> Any:
    table = pa.table(columns)
    return pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=1))


def _routes(tmp_path: Path) -> dict[str, tuple[Callable[..., dp.ProfileReport], str, str, str]]:
    """route -> (profile one table, source_type, source_format, outcome)."""

    def file(suffix: str, write: Callable[[dict, Path], object]) -> Callable[..., dp.ProfileReport]:
        def run(columns: dict, max_rows: int | None = None) -> dp.ProfileReport:
            path = tmp_path / f"{'note' if 'note' in columns else 'no_note'}{suffix}"
            write(columns, path)
            return dp.profile(str(path), max_rows=max_rows)

        return run

    return {
        "csv file": (
            file(".csv", lambda c, p: p.write_bytes(_csv(c))),
            "file",
            "csv",
            "failed",
        ),
        "parquet file": (
            file(".parquet", lambda c, p: pq.write_table(pa.table(c), p)),
            "file",
            "parquet",
            "failed",
        ),
        "jsonl file": (
            file(".jsonl", lambda c, p: p.write_bytes(_jsonl(c))),
            "file",
            "jsonl",
            "evidence_incomplete:truncated",
        ),
        "csv bytes": (
            lambda c, max_rows=None: dp.profile(_csv(c), format="csv", max_rows=max_rows),
            "bytes",
            "csv",
            "failed",
        ),
        "arrow stream": (
            lambda c, max_rows=None: dp.profile(_reader(c), max_rows=max_rows),
            "stream",
            "arrow",
            "failed",
        ),
    }


def _note_outcome(current: dp.ProfileReport, baseline: dp.ProfileReport) -> str:
    result = current.check(baseline=baseline, forbid_added_columns=True)
    (check,) = [check for check in result.checks if check.column == "note"]
    if check.status != "not_evaluated":
        return check.status
    assert check.reason is not None
    return f"{check.reason['reason']}:{check.reason.get('gap')}"


def test_a_capped_summary_baseline_declares_its_columns_where_its_format_does(
    tmp_path: Path,
) -> None:
    wrong = []
    for route, (profile, source_type, source_format, expected) in _routes(tmp_path).items():
        current = profile(ORDERS)
        capped = profile(NO_NOTE, max_rows=1)
        if capped.source_exhausted:
            wrong.append((route, "the cap did not stop the scan"))
        if capped.source_format != source_format:
            wrong.append((route, "native source_format", capped.source_format))
        summary = capped.to_dict()
        if (summary["source_type"], summary.get("source_format")) != (source_type, source_format):
            wrong.append((route, "summary", summary["source_type"], summary.get("source_format")))
        loaded = dp.ProfileReport.from_dict(summary)
        if loaded.source_format != source_format:
            wrong.append((route, "loaded source_format", loaded.source_format))
        if loaded.to_dict().get("source_format") != source_format:
            wrong.append((route, "resaved summary", loaded.to_dict().get("source_format")))
        outcome = _note_outcome(current, loaded)
        if outcome != expected:
            wrong.append((route, outcome, f"expected {expected}"))
        # A summary written before the field covers only the rows read.
        del summary["source_format"]
        legacy = dp.ProfileReport.from_dict(summary)
        if legacy.source_format is not None or "source_format" in legacy.to_dict():
            wrong.append((route, "legacy summary gained a format"))
        outcome = _note_outcome(current, legacy)
        if outcome != "evidence_incomplete:truncated":
            wrong.append((route, "without source_format", outcome))
    assert not wrong, wrong


def test_a_malformed_source_format_is_refused_and_null_reads_as_absent() -> None:
    summary = dp.profile(_csv(NO_NOTE), format="csv").to_dict()
    summary["source_format"] = 5
    with pytest.raises(ValueError, match="'source_format' must be a string"):
        dp.ProfileReport.from_dict(summary)
    summary["source_format"] = None
    loaded = dp.ProfileReport.from_dict(summary)
    assert loaded.source_format is None
    assert "source_format" not in loaded.to_dict()
