"""A column with no non-null value carries no statistics (#983).

An all-null text column used to report lengths of 0, the same as a column of
real zero-length values, while an all-null numeric column already reported no
statistics. `tests/all_null_columns.rs` is the Rust twin over the file engines;
this covers the in-memory routes. The oracle (`test_oracle.py`, `all_null`)
checks the same case on every route against an independent answer.
"""

from __future__ import annotations

import dataprof as dp
import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")
pl = pytest.importorskip("polars")

EMPTY = [None, None, None]
BLANK = ["NA", "null", ""]
NOTE = [None, "x", None]


def _routes():
    frame = {"id": [1, 2, 3], "empty": EMPTY, "blank": BLANK, "note": NOTE}
    records = [dict(zip(frame, row)) for row in zip(*frame.values())]
    table = pa.table(
        {
            "id": [1, 2, 3],
            "empty": pa.array(EMPTY, pa.string()),
            "blank": BLANK,
            "note": NOTE,
        }
    )
    return {
        "dict": lambda: dp.profile(frame),
        "records": lambda: dp.profile(records),
        "pandas": lambda: dp.profile(pd.DataFrame(frame)),
        "arrow": lambda: dp.profile(table),
        "polars": lambda: dp.profile(pl.from_arrow(table)),
    }


def test_a_column_of_nulls_reports_no_statistics_and_keeps_its_counts():
    wrong = []
    for route, run in _routes().items():
        report = run()
        for name in ("empty", "blank"):
            column = report[name]
            exported = report.to_dict()["columns"][list(report.column_profiles).index(name)]
            if exported.get("stats") is not None or column.min_length is not None:
                wrong.append(f"{route}: {name} stats {exported.get('stats')}")
            if (column.null_count, column.total_count) != (3, 3):
                wrong.append(f"{route}: {name} counts {column.null_count}/{column.total_count}")
        note = report["note"]
        if (note.min_length, note.max_length) != (1, 1):
            wrong.append(f"{route}: note lengths {note.min_length}/{note.max_length}")
    assert not wrong, "\n".join(wrong)


def test_an_identifier_column_of_nulls_reports_no_statistics():
    report = dp.profile({"id": [1, 2], "code": [None, None]}, identifier_columns=["code"])
    assert report.to_dict()["columns"][1].get("stats") is None
    assert report["code"].min_length is None
