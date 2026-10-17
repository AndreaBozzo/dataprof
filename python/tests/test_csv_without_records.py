"""A CSV file that holds no record at all: empty, or nothing but blank lines.

Blank lines are skipped, so such a file has no header and no rows, and every
engine reports an empty profile. ``engine="columnar"`` used to raise
``ValueError: incorrect number of fields for line 1`` on the blank-lines-only
file, which Arrow decoded against a schema of zero fields (#964).

``tests/csv_without_records.rs`` is the Rust twin.
"""

from __future__ import annotations

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

#: Files with no record, by label. The empty file is the one every engine
#: already agreed on, and the reference for the others.
FILES = {
    "empty": b"",
    "BOM then a blank line": b"\xef\xbb\xbf\n",
    "LF blank lines": b"\n\n\n",
    "CRLF blank lines": b"\r\n\r\n",
    "CR blank lines": b"\r\r",
    "one blank line": b"\n",
}

#: The cap and flexible parsing take their own paths through the columnar engine.
READS: dict[str, dict[str, Any]] = {
    "default": {},
    "row cap": {"max_rows": 10},
    "flexible": {"csv_flexible": True},
}

ENGINES = ("auto", "incremental", "columnar")


def _summary(report: dp.ProfileReport) -> dict[str, Any]:
    """What the engines must agree on; engine provenance is left out."""
    data = report.to_dict()
    execution = data["execution"]
    return {
        "columns": data["columns"],
        "quality": data["quality"],
        "quality_status": data["quality_status"],
        "rows_processed": execution["rows_processed"],
        "columns_detected": execution["columns_detected"],
    }


def test_a_csv_without_records_profiles_empty_on_every_engine(tmp_path: Path):
    reference_path = tmp_path / "reference.csv"
    reference_path.write_bytes(b"")
    expected = _summary(dp.profile(str(reference_path), engine="incremental"))
    assert expected["rows_processed"] == 0
    assert expected["columns_detected"] == 0

    wrong = []
    for index, (label, content) in enumerate(FILES.items()):
        path = tmp_path / f"file_{index}.csv"
        path.write_bytes(content)
        for read, options in READS.items():
            for engine in ENGINES:
                try:
                    found = _summary(dp.profile(str(path), engine=engine, **options))
                except ValueError as error:
                    wrong.append(f"[{label}, {read}] {engine} raised: {error}")
                    continue
                if found != expected:
                    wrong.append(f"[{label}, {read}] {engine}: {found} != {expected}")
    assert not wrong, "\n".join(wrong)
