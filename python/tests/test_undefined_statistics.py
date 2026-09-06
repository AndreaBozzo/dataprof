"""Undefined aggregates stay absent, while measured zeros survive every path (#667)."""

import dataprof
import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")


@pytest.fixture(params=["auto", "incremental", "columnar", "arrow", "parquet"])
def profile_values(request, tmp_path):
    def profile(integer, number, boolean):
        table = pa.table(
            {
                "i": pa.array(integer, type=pa.int64()),
                "f": pa.array(number, type=pa.float64()),
                "b": pa.array(boolean, type=pa.bool_()),
            }
        )
        if request.param == "arrow":
            return dataprof.profile(table)
        if request.param == "parquet":
            path = tmp_path / "values.parquet"
            pq.write_table(table, path)
            return dataprof.profile(path)
        path = tmp_path / "values.csv"
        rows = [
            ",".join("" if value is None else str(value) for value in row)
            for row in zip(integer, number, boolean)
        ]
        path.write_text("i,f,b\n" + "\n".join(rows) + "\n", encoding="utf-8")
        return dataprof.profile(path, engine=request.param)

    return profile


def test_all_null_columns_have_no_statistics(profile_values):
    report = profile_values([None, None], [None, None], [None, None])
    for column in report.to_dict()["columns"]:
        assert column["total_count"] == 2
        assert column["null_count"] == 2
        # CSV has no declared types: an all-null column infers as string.
        # Arrow and Parquet retain the numeric/boolean schema and must omit stats.
        if column["data_type"] != "string":
            assert column.get("stats") is None
        for field in ("min", "max", "mean", "std_dev", "variance", "true_ratio"):
            assert getattr(report[column["name"]], field) is None


def test_non_finite_numbers_have_no_statistics(profile_values):
    report = profile_values([None, None], [float("inf"), float("-inf")], [None, None])
    column = report["f"]
    assert column.data_type == "float"
    assert column.total_count == 2
    assert column.null_count == 0
    assert column.invalid_count == 2
    assert column.min is None
    assert report.to_dict()["columns"][1].get("stats") is None


def test_measured_zeros_survive_null_and_invalid_values(profile_values):
    report = profile_values([None, 0, 0], [float("inf"), 0.0, 0.0], [None, False, False])
    for name in ("i", "f"):
        for field in ("min", "max", "mean", "std_dev", "variance"):
            assert getattr(report[name], field) == 0.0
    assert report["f"].invalid_count == 1
    assert report["b"].true_ratio == 0.0
    assert report["b"].true_count == 0
    assert report["b"].false_count == 2


def test_declared_types_and_absence_survive_report_roundtrip():
    table = pa.table(
        {"i": pa.array([None], type=pa.int64()), "b": pa.array([None], type=pa.bool_())}
    )
    report = dataprof.profile(table)
    restored = dataprof.ProfileReport.from_dict(report.to_dict())
    assert restored["i"].data_type == "integer"
    assert restored["b"].data_type == "boolean"
    assert restored["i"].min is None
    assert restored["b"].true_ratio is None
    assert restored.to_dict()["columns"] == report.to_dict()["columns"]
