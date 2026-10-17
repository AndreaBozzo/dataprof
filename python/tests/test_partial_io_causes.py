"""Partial APIs retain I/O causes. Rust twin: tests/partial_io_causes.rs."""

from pathlib import Path

import dataprof as dp


def test_partial_apis_preserve_non_missing_io_causes(tmp_path: Path):
    wrong = []
    for extension in ("csv", "json", "jsonl", "parquet"):
        directory = tmp_path / f"directory.{extension}"
        directory.mkdir()
        missing = tmp_path / f"missing.{extension}"
        for api in (dp.quick_row_count, dp.infer_schema, dp.analyze_structure):
            for path in (directory, missing):
                try:
                    api(path)
                except Exception as error:
                    if isinstance(error, FileNotFoundError) != (path == missing):
                        wrong.append(
                            f"[{api.__name__}] {path.name}: {type(error).__name__}: {error}"
                        )
                else:
                    wrong.append(f"[{api.__name__}] {path.name}: succeeded unexpectedly")
    assert not wrong, "\n".join(wrong)
