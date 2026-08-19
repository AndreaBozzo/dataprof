"""`complete_records_ratio` counts records, not null cells.

Every input path reaches rows differently — a dict of columns, a list of
records, a pandas frame handed over as Arrow batches — and each has to arrive
at the same figure for the same data. Deriving it from per-column null totals
does not: that assumes no two nulls share a record, which understates
completeness on exactly the datasets it describes, and reports 0% once the
null cells outnumber the rows.
"""

from __future__ import annotations

import dataprof
import pytest

# Four records, two of them missing both optional fields. Two records are
# complete, so the answer is 50.0. The null cells number 4 across 4 rows,
# which a cell-based bound reads as 0%.
COLUMNS: dict[str, list] = {
    "id": [1, 2, 3, 4],
    "notes": [None, None, "ok", "ok"],
    "city": [None, None, "Rome", "Rome"],
}
EXPECTED_RATIO = 50.0


def completeness(report) -> dict:
    return report.quality.completeness


def test_dict_of_columns_counts_complete_records() -> None:
    report = dataprof.profile(COLUMNS)
    assert completeness(report)["complete_records_ratio"] == pytest.approx(EXPECTED_RATIO)


def test_list_of_records_counts_complete_records() -> None:
    records = [
        {name: values[row] for name, values in COLUMNS.items()} for row in range(len(COLUMNS["id"]))
    ]
    report = dataprof.profile(records)
    assert completeness(report)["complete_records_ratio"] == pytest.approx(EXPECTED_RATIO)


def test_null_like_strings_count_as_missing_fields() -> None:
    # The column counters read these as nulls, so the record count must too.
    report = dataprof.profile({"a": ["x", "y"], "b": ["NULL", "1"]})
    assert completeness(report)["complete_records_ratio"] == pytest.approx(50.0)


def test_dataframe_agrees_with_the_dict_it_came_from() -> None:
    pandas = pytest.importorskip("pandas")

    frame = pandas.DataFrame(COLUMNS)
    report = dataprof.profile(frame)
    assert completeness(report)["complete_records_ratio"] == pytest.approx(EXPECTED_RATIO)


def test_csv_file_agrees_with_the_same_data_in_memory(tmp_path) -> None:
    path = tmp_path / "records.csv"
    rows = ["id,notes,city", "1,,", "2,,", "3,ok,Rome", "4,ok,Rome"]
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")

    from_file = dataprof.profile(str(path))
    from_memory = dataprof.profile(COLUMNS)
    assert completeness(from_file)["complete_records_ratio"] == pytest.approx(
        completeness(from_memory)["complete_records_ratio"]
    )
    assert completeness(from_file)["complete_records_ratio"] == pytest.approx(EXPECTED_RATIO)
