"""Start/end dates are compared only when they come from the same row (#787).

The quality calculators read each column's retained values, which hold non-null
values only, so once either column of a pair has a null, zipping them compares
dates from different rows. Such a pair is now not compared and the ordering is
reported as not assessed. The Rust twin is ``tests/temporal_pairs.rs``; this
file adds the in-memory routes.
"""

from __future__ import annotations

from pathlib import Path

import dataprof as dp
import pytest

ISSUE_EXAMPLE = "start_date,end_date\n,2024-01-02\n2024-01-05,2024-01-06\n2024-01-10,2024-01-11\n"
ENGINES = ("auto", "incremental", "columnar")


def _pairs(report: dp.ProfileReport) -> tuple[int, int]:
    assert report.quality is not None
    timeliness = report.quality.timeliness
    assert timeliness is not None
    return timeliness["temporal_pairs_checked"], timeliness["temporal_violations"]


def _codes(report: dp.ProfileReport) -> tuple[set[str], set[str]]:
    result = report.findings()
    return {f.code for f in result.findings}, {r["code"] for r in result.not_evaluated}


@pytest.mark.parametrize("engine", ENGINES)
def test_a_pair_with_nulls_is_not_compared(tmp_path: Path, engine: str):
    path = tmp_path / "pairs.csv"
    path.write_text(ISSUE_EXAMPLE)
    report = dp.profile_file(path, engine=engine)
    assert _pairs(report) == (0, 0)
    found, not_evaluated = _codes(report)
    assert "temporal_order_violations" not in found
    assert "temporal_order_violations" in not_evaluated


def _big_rows() -> list[dict[str, str]]:
    # Every end date is the day after its start; 20,000 rows is past the
    # 10,000-value reservoir, where each column samples on its own.
    rows = []
    for index in range(20_000):
        day = index * 7_919 % 2_000
        year, month, dom = 2020 + day // 324, 1 + day % 324 // 27, 1 + day % 27
        rows.append(
            {
                "start_date": f"{year}-{month:02}-{dom:02}",
                "end_date": f"{year}-{month:02}-{dom + 1:02}",
                "note": "x" if index % 3 == 0 else "",
            }
        )
    return rows


def test_null_free_columns_keep_the_same_rows_on_every_in_memory_route(tmp_path: Path):
    pd = pytest.importorskip("pandas")
    pa = pytest.importorskip("pyarrow")
    rows = _big_rows()
    frame = pd.DataFrame(rows)
    sources = {
        "pandas": frame,
        "arrow": pa.Table.from_pandas(frame),
        "dicts": rows,
    }
    try:
        import polars as pl

        sources["polars"] = pl.from_pandas(frame)
    except ImportError:
        pass
    jsonl = tmp_path / "pairs.jsonl"
    frame.to_json(jsonl, orient="records", lines=True)
    for name, source in sources.items():
        checked, violations = _pairs(dp.profile(source))
        # Row dicts keep every non-null value; the others keep a 10,000-value sample.
        assert checked == (20_000 if name == "dicts" else 10_000), name
        assert violations == 0, f"{name} paired values from different rows"
    assert _pairs(dp.profile_file(jsonl)) == (10_000, 0)
