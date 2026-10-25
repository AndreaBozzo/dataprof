"""A UTF-8 BOM never changes what a chunked CSV read finds.

With a BOM and a blank line before the header, ``chunk_size=1`` on the
incremental engine read an empty header and the real one as data: no columns,
a row too many, and ``max_null_percentage=0`` and ``min_rows=2`` passed (#1101).
Every chunk size must read what the unchunked read does.

``tests/csv_bom_chunks.rs`` is the Rust twin.
"""

from __future__ import annotations

from pathlib import Path

import pytest

try:
    import dataprof as dp
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

#: label -> (bytes, expected columns as (name, null_count), rows)
FILES: dict[str, tuple[bytes, list[tuple[str, int]], int]] = {
    "BOM, blank line": (b"\xef\xbb\xbf\na;b\n1;\n", [("a", 0), ("b", 1)], 1),
    "BOM, blank lines": (b"\xef\xbb\xbf\n\r\n\na;b\n1;\n2;3\n", [("a", 0), ("b", 1)], 2),
    "BOM, quoted multi-line header": (
        b'\xef\xbb\xbf"a\nb";c\n1;\n2;3\n',
        [("a\nb", 0), ("c", 1)],
        2,
    ),
    "BOM, no blank line": (b"\xef\xbb\xbfa;b\n1;\n", [("a", 0), ("b", 1)], 1),
}


def test_every_chunk_size_reads_the_whole_header(tmp_path: Path) -> None:
    wrong = {}
    for label, (data, columns, rows) in FILES.items():
        path = tmp_path / "probe.csv"
        path.write_bytes(data)
        for engine in ("incremental", "auto"):
            for size in range(1, len(data) + 2):
                report = dp.profile(path, engine=engine, chunk_size=size)
                shape = (
                    [(c["name"], c["null_count"]) for c in report.to_dict()["columns"]],
                    report.rows,
                )
                if shape != (columns, rows):
                    wrong[f"[{label}] {engine} chunk {size}"] = shape
    assert not wrong, wrong


def test_the_reported_gates_fail_at_chunk_size_one(tmp_path: Path) -> None:
    path = tmp_path / "probe.csv"
    path.write_bytes(b"\xef\xbb\xbf\na;b\n1;\n")
    report = dp.profile(path, engine="incremental", chunk_size=1)
    assert report.check(max_null_percentage=0).verdict == "fail"
    assert report.check(min_rows=2).verdict == "fail"
