"""A progress callback hears from every file route, starting with the default one.

The Python guide's example, ``dp.profile("data.csv", on_progress=...)``, runs
the default engine, and it used to deliver no event at all: only
``engine="incremental"`` reported progress.
"""

from __future__ import annotations

from pathlib import Path

import dataprof as dp
import pytest

EXAMPLE_PARQUET = Path(__file__).resolve().parents[2] / "examples" / "test_data" / "simple.parquet"


@pytest.fixture
def csv_path(tmp_path: Path) -> Path:
    path = tmp_path / "orders.csv"
    path.write_text("id,amount\n" + "".join(f"{i},{i % 97}\n" for i in range(2_000)))
    return path


def _events(path: Path, **kwargs):
    events = []
    report = dp.profile(path, on_progress=events.append, **kwargs)
    return report, [event.kind for event in events], events


def test_the_default_engine_reports_progress(csv_path: Path):
    report, kinds, events = _events(csv_path)

    assert kinds[0] == "started"
    assert "schema_detected" in kinds
    assert kinds[-1] == "finished"
    assert events[-1].total_rows == report.rows == 2_000


@pytest.mark.parametrize(
    ("label", "kwargs"),
    [("columnar", {"engine": "columnar"}), ("jsonl", {}), ("parquet", {})],
)
def test_routes_that_do_not_stream_are_bracketed(tmp_path: Path, csv_path: Path, label, kwargs):
    path = {
        "columnar": csv_path,
        "jsonl": tmp_path / "orders.jsonl",
        "parquet": EXAMPLE_PARQUET,
    }[label]
    if label == "jsonl":
        path.write_text('{"id": 1}\n{"id": 2}\n')

    report, kinds, events = _events(path, **kwargs)

    assert kinds == ["started", "finished"]
    assert events[-1].total_rows == report.rows
    assert events[-1].truncated is False
