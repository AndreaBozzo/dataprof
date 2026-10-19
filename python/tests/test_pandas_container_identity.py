"""Pandas mixed-object fallback preserves container identity in row signatures."""

from __future__ import annotations

import json
import sys
from collections.abc import Sequence
from decimal import Decimal

import dataprof as dp
import pytest

pd = pytest.importorskip("pandas", reason="pandas is required for pandas DataFrame tests")
pa = pytest.importorskip("pyarrow", reason="pyarrow is required for Arrow import tests")


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


@pytest.mark.slow
def test_container_identity_spill_does_not_claim_exact_quality():
    # The native fallback folds each container/string pair into one row. The
    # repaired identities cross the 1,000,000-distinct-row exact-set limit,
    # while the original report still has exact duplicate provenance.
    pairs = 500_001
    df = pd.DataFrame(
        {
            "neighbor": pd.RangeIndex(pairs).repeat(2),
            "payload": pd.Series([{"x": 1}, '{"x":1}'] * pairs, dtype=object),
        }
    )

    report = dp.profile(df, quality_dimensions=["uniqueness"])

    assert _uniqueness(report)["duplicate_rows_approximate"] is True
    assert report.quality_sampled_dimensions == ["duplicate_rows"]
    confidence = json.loads(report.to_json())["quality"]["confidence"]["Mixed"]
    assert confidence["exact_dimensions"] == []
    # Without a bound for the repaired sketch, its high point estimate cannot
    # establish either a whole-source dimension or overall score floor.
    assert (
        report.check(min_quality_score=90, min_dimension_scores={"uniqueness": 90}).verdict
        == "inconclusive"
    )


def test_duplicate_replacement_preserves_other_quality_provenance_and_bounds():
    from dataprof._dataprof import _replace_row_duplicates, profile_dataframe

    # This small report already has Mixed confidence from the 10,000-value
    # quality reservoir. Exercise both state transitions at the native helper
    # boundary without multiplying the million-row public-path fixture.
    df = pd.DataFrame({"id": range(10_001), "value": ["ok"] * 10_001})
    config = dp.ProfilerConfig(quality_dimensions=["completeness", "consistency", "uniqueness"])
    native = profile_dataframe(df, "dataframe", None, config)
    report = dp.ProfileReport(native)
    original = json.loads(report.to_json())["quality"]
    original_confidence = original["confidence"]["Mixed"]
    original_bounds = original["score_bounds"]

    _replace_row_duplicates(native, 0, len(df), True)

    updated = json.loads(report.to_json())["quality"]
    confidence = updated["confidence"]["Mixed"]
    assert confidence["sample_size"] == original_confidence["sample_size"]
    assert confidence["exact_dimensions"] == ["completeness", "key_uniqueness"]
    assert confidence["sampled_dimensions"] == ["consistency", "duplicate_rows"]
    assert updated["metrics"]["completeness"] == original["metrics"]["completeness"]
    assert updated["metrics"]["consistency"] == original["metrics"]["consistency"]
    assert updated["scores"] == original["scores"]
    assert updated["score_bounds"]["overall_score"] is None
    for dimension, interval in original_bounds["dimension_scores"].items():
        assert updated["score_bounds"]["dimension_scores"][dimension] == (
            None if dimension == "uniqueness" else interval
        )

    _replace_row_duplicates(native, 0, len(df), False)

    restored = json.loads(report.to_json())["quality"]
    assert restored["confidence"] == original["confidence"]
    assert restored["metrics"] == original["metrics"]
    assert report.check(min_dimension_scores={"uniqueness": 90}).verdict == "pass"


def test_arrow_compatible_container_column_keeps_native_path():
    from dataprof._pandas_mixed import _pandas_container_fallback_table

    df = pd.DataFrame({"c": pd.Series([[1, 2], [3, 4]], dtype=object)})

    assert _pandas_container_fallback_table(df) is None


def test_duplicate_only_native_helper_matches_full_column_profiler():
    from dataprof._dataprof import _count_container_aware_duplicates, profile_columns

    columns: list[tuple[str, list[str | None], list[int]]] = [
        ("id", ["a", "a", "b", "b", "b"], []),
        ("payload", ['{"x":1}'] * 5, [0, 2]),
    ]
    table = pa.table(
        {
            "id": columns[0][1],
            "payload": columns[1][1],
            "payload_container": [True, False, True, False, False],
        }
    )
    dedicated = _count_container_aware_duplicates(table)
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

    assert _count_container_aware_duplicates(pa.table({})) is None
    assert _count_container_aware_duplicates(pa.table({"payload": []})) is None


@pytest.mark.parametrize(
    "table",
    [None, {}, []],
)
def test_duplicate_only_native_helper_rejects_non_table_sources(table):
    from dataprof._dataprof import _count_container_aware_duplicates

    with pytest.raises(TypeError, match="Expected a pyarrow Table or RecordBatch"):
        _count_container_aware_duplicates(table)


@pytest.mark.parametrize("entrypoint", ["profile", "interop"])
@pytest.mark.parametrize("max_rows", [None, 2])
@pytest.mark.parametrize(
    "neighbors",
    [
        [1, 1.0, 1],
        [Decimal("1.0"), Decimal("1.00"), Decimal("1.0")],
        [
            pd.Timestamp("2020-01-01 00:00Z"),
            pd.Timestamp("2020-01-01 01:00+01:00"),
            pd.Timestamp("2020-01-01 00:00Z"),
        ],
        [{"id": 1}, {"id": 1, "optional": None}, {"id": 1}],
    ],
    ids=["numeric-coercion", "decimal-scale", "timezone", "struct-schema"],
)
def test_fallback_preserves_native_neighbor_row_identity(entrypoint, max_rows, neighbors):
    from dataprof._dataprof import profile_dataframe as native_profile_dataframe

    df = pd.DataFrame(
        {
            "neighbor": pd.Series(neighbors, dtype=object),
            "payload": pd.Series([{"x": 1}, {"x": 1}, '{"x":1}'], dtype=object),
        }
    )
    # Arrow gives the first two neighboring cells one logical value, even
    # though their original Python spellings differ. Correcting the payload
    # must preserve this real duplicate, with or without the row cap.
    native = dp.ProfileReport(native_profile_dataframe(df.iloc[:2], "dataframe", None, None))
    assert _uniqueness(native)["duplicate_rows"] == 1

    if entrypoint == "profile":
        report = dp.profile(df, max_rows=max_rows)
    else:
        from dataprof.interop import profile_dataframe

        report = dp.ProfileReport(profile_dataframe(df, max_rows=max_rows))

    assert _uniqueness(report)["duplicate_rows"] == 1
    assert _uniqueness(report)["rows_checked"] == (max_rows or 3)


def test_fallback_preserves_native_map_equality():
    # Arrow maps have no entry-order semantics. Re-stringifying Python lists
    # of key/value pairs would incorrectly separate the first two rows.
    values = [
        [("a", 1), ("b", 2)],
        [("b", 2), ("a", 1)],
        [("a", 1), ("b", 2)],
    ]
    df = pd.DataFrame(
        {
            "mapping": pd.Series(values, dtype=pd.ArrowDtype(pa.map_(pa.string(), pa.int64()))),
            "payload": pd.Series([{"x": 1}, {"x": 1}, '{"x":1}'], dtype=object),
        }
    )

    assert _uniqueness(dp.profile(df))["duplicate_rows"] == 1


def test_fallback_markers_do_not_collide_with_source_columns():
    df = pd.DataFrame(
        {
            "__dataprof_container_0": ["same"] * 3,
            "payload": pd.Series([{"x": 1}, {"x": 1}, '{"x":1}'], dtype=object),
        }
    )

    report = dp.profile(df)

    assert list(report) == list(df.columns)
    assert _uniqueness(report)["duplicate_rows"] == 1


def test_fallback_checks_no_container_values_beyond_the_row_cap(monkeypatch):
    from dataprof import _pandas_mixed

    omitted = {"beyond": "the row cap"}
    df = pd.DataFrame({"c": pd.Series(["a", "b", omitted], dtype=object)})
    is_container = _pandas_mixed._is_container

    def check_analyzed_value(value):
        assert value is not omitted, "container detection visited an unanalyzed row"
        return is_container(value)

    monkeypatch.setattr(_pandas_mixed, "_is_container", check_analyzed_value)

    assert _pandas_mixed._pandas_container_fallback_table(df, 2) is None


@pytest.mark.skipif(sys.maxsize < (1 << 63) - 1, reason="requires a 64-bit native row cap")
@pytest.mark.parametrize("entrypoint", ["profile", "interop"])
def test_fallback_accepts_row_caps_larger_than_arrow_slice_length(entrypoint):
    # The native binding accepts usize, while Arrow slice lengths use signed
    # int64. A cap beyond the input still means analyze the entire small frame.
    df = pd.DataFrame({"payload": pd.Series([{"x": 1}, '{"x":1}'], dtype=object)})
    if entrypoint == "profile":
        report = dp.profile(df, max_rows=1 << 63)
    else:
        from dataprof.interop import profile_dataframe

        report = dp.ProfileReport(profile_dataframe(df, max_rows=1 << 63))

    assert report.rows == 2
    assert _uniqueness(report)["duplicate_rows"] == 0
