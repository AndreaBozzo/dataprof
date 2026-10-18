"""Pandas mixed-object fallback preserves container identity in row signatures."""

from __future__ import annotations

from collections.abc import Sequence

import dataprof as dp
import pytest

pd = pytest.importorskip("pandas", reason="pandas is required for pandas DataFrame tests")
pytest.importorskip("pyarrow", reason="pyarrow is required for Arrow import tests")


def _report(entrypoint: str, values: Sequence[object]) -> dp.ProfileReport:
    df = pd.DataFrame({"c": pd.Series(values, dtype=object)})
    if entrypoint == "profile":
        return dp.profile(df)

    from dataprof.interop import profile_dataframe

    return dp.ProfileReport(profile_dataframe(df))


def _uniqueness(report: dp.ProfileReport):
    quality = report.quality
    assert quality is not None
    uniqueness = quality.uniqueness
    assert uniqueness is not None
    return uniqueness


@pytest.mark.parametrize("entrypoint", ["profile", "interop"])
def test_container_and_json_spelling_are_distinct_rows(entrypoint: str):
    values = [{"x": 1}, '{"x":1}', [1, 2], "[1,2]"]

    report = _report(entrypoint, values)

    assert _uniqueness(report)["duplicate_rows"] == 0


@pytest.mark.parametrize("entrypoint", ["profile", "interop"])
def test_equal_containers_and_equal_strings_still_duplicate(entrypoint: str):
    values = [{"x": 1}, {"x": 1}, '{"x":1}', '{"x":1}']

    report = _report(entrypoint, values)

    assert _uniqueness(report)["duplicate_rows"] == 2


def test_fallback_does_not_retype_neighboring_native_columns():
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "active": [True, False, True, False],
            "c": pd.Series([{"x": 1}, '{"x":1}', [1, 2], "[1,2]"], dtype=object),
        }
    )

    report = dp.profile(df)

    assert report["id"].data_type == "integer"
    assert report["active"].data_type == "boolean"
    assert _uniqueness(report)["duplicate_rows"] == 0


def test_container_identity_side_pass_honors_max_rows():
    df = pd.DataFrame(
        {
            "c": pd.Series(
                [{"x": 1}, '{"x":1}', {"x": 1}, {"x": 1}],
                dtype=object,
            )
        }
    )

    report = dp.profile(df, max_rows=2)

    assert report.rows == 2
    assert _uniqueness(report)["rows_checked"] == 2
    assert _uniqueness(report)["duplicate_rows"] == 0


def test_duplicate_repair_refreshes_quality_score():
    from dataprof._dataprof import profile_dataframe as native_profile_dataframe

    df = pd.DataFrame({"c": pd.Series([{"x": 1}, '{"x":1}', [1, 2], "[1,2]"], dtype=object)})
    native = dp.ProfileReport(native_profile_dataframe(df, "dataframe", None, None))
    repaired = dp.profile(df)

    assert _uniqueness(native)["duplicate_rows"] == 2
    assert _uniqueness(repaired)["duplicate_rows"] == 0
    native_score = native.quality_score
    repaired_score = repaired.quality_score
    assert native_score is not None
    assert repaired_score is not None
    assert repaired_score > native_score


def test_arrow_compatible_container_column_keeps_native_path():
    from dataprof._pandas_mixed import _pandas_container_fallback_columns

    df = pd.DataFrame({"c": pd.Series([[1, 2], [3, 4]], dtype=object)})

    assert _pandas_container_fallback_columns(df) is None


def test_duplicate_only_native_helper_matches_full_column_profiler():
    from dataprof._dataprof import _count_container_aware_duplicates, profile_columns

    columns: list[tuple[str, list[str | None], list[int]]] = [
        ("id", ["a", "a", "b", "b", "b"], []),
        ("payload", ['{"x":1}'] * 5, [0, 2]),
    ]
    dedicated = _count_container_aware_duplicates(columns)
    original = profile_columns(columns, "example", None, None)
    assert original.quality is not None
    uniqueness = original.quality.uniqueness
    assert uniqueness is not None

    assert dedicated == (
        uniqueness["duplicate_rows"],
        uniqueness["rows_checked"],
        uniqueness["duplicate_rows_approximate"],
    )
    assert dedicated == (1, 5, False)


def test_duplicate_only_native_helper_handles_no_rows_or_columns():
    from dataprof._dataprof import _count_container_aware_duplicates

    assert _count_container_aware_duplicates([]) is None
    assert _count_container_aware_duplicates([("payload", [], [])]) is None


@pytest.mark.parametrize(
    "columns",
    [
        [("a", ["x"], []), ("b", ["x", "y"], [])],
        [("a", ["x", "y"], [1, 0])],
        [("a", ["x"], [1])],
    ],
)
def test_duplicate_only_native_helper_rejects_invalid_cell_metadata(columns):
    from dataprof._dataprof import _count_container_aware_duplicates

    with pytest.raises(ValueError, match="duplicate signature:"):
        _count_container_aware_duplicates(columns)
