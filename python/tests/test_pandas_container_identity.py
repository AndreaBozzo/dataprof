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


def test_container_identity_honors_max_rows():
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


@pytest.mark.slow
def test_container_identity_spill_preserves_provenance_and_certified_bounds():
    # Container-aware identities cross the 1,000,000-distinct-row exact-set
    # limit. The report must derive its provenance and bounds from that sketch,
    # even though collapsing each container/string pair would stay exact.
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
    quality = json.loads(report.to_json())["quality"]
    confidence = quality["confidence"]["Mixed"]
    assert confidence["exact_dimensions"] == []
    # Before spilling, the exact set already certified more than a million
    # distinct rows. Keep that real floor together with the approximate flag;
    # the point estimate alone still cannot establish perfect uniqueness.
    interval = quality["score_bounds"]["dimension_scores"]["uniqueness"]
    assert 90 <= interval["lower"] < interval["upper"] == 100
    assert (
        report.check(min_quality_score=90, min_dimension_scores={"uniqueness": 90}).verdict
        == "pass"
    )
    assert (
        report.check(min_quality_score=100, min_dimension_scores={"uniqueness": 100}).verdict
        == "inconclusive"
    )


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


@pytest.mark.parametrize("entrypoint", ["profile", "interop"])
def test_mixed_frame_serializes_fallback_cells_once(entrypoint):
    rendered = []

    class Scalar:
        def __str__(self):
            rendered.append("scalar")
            return "scalar"

    df = pd.DataFrame({"payload": pd.Series([{"x": 1}, '{"x":1}', Scalar()], dtype=object)})
    if entrypoint == "profile":
        report = dp.profile(df)
    else:
        from dataprof.interop import profile_dataframe

        report = dp.ProfileReport(profile_dataframe(df))

    assert _uniqueness(report)["duplicate_rows"] == 0
    assert rendered == ["scalar"]


def test_native_dataframe_counts_container_identity_before_report_assembly():
    from dataprof._dataprof import profile_dataframe

    df = pd.DataFrame({"payload": pd.Series([{"x": 1}, '{"x":1}', [1, 2], "[1,2]"], dtype=object)})
    report = dp.ProfileReport(profile_dataframe(df))

    assert _uniqueness(report)["duplicate_rows"] == 0


def test_exact_duplicate_bounds_use_container_aware_counts():
    values = [
        {"x": row // 2 % 7} if row % 2 == 0 else '{"x":' + str(row // 2 % 7) + "}"
        for row in range(20_000)
    ]
    report = dp.profile(
        pd.DataFrame({"payload": pd.Series(values, dtype=object)}),
        quality_dimensions=["completeness", "consistency", "uniqueness"],
    )
    quality = json.loads(report.to_json())["quality"]

    assert _uniqueness(report)["duplicate_rows"] == 20_000 - 14
    assert _uniqueness(report)["duplicate_rows_approximate"] is False
    assert quality["score_bounds"]["dimension_scores"]["uniqueness"] == {
        "lower": 0.07,
        "upper": 0.07,
    }
    confidence = quality["confidence"]["Mixed"]
    assert "duplicate_rows" in confidence["exact_dimensions"]
    assert confidence["sampled_dimensions"] == ["consistency"]


def test_arrow_compatible_containers_keep_native_types_without_fallback_markers():
    from dataprof._inputs import _pandas_to_pyarrow_table

    df = pd.DataFrame({"payload": pd.Series([[1, 2], [3, 4]], dtype=object)})
    table, markers = _pandas_to_pyarrow_table(df, pa, container_markers=True)

    assert pa.types.is_list(table.schema.field("payload").type)
    assert markers == {}
    assert isinstance(_pandas_to_pyarrow_table(df, pa), pa.Table)


def test_fallback_metadata_uses_original_column_positions_and_missing_values():
    from dataprof._inputs import _pandas_to_pyarrow_table

    df = pd.DataFrame(
        {
            "native": [1, 2, 3, 4],
            "mixed": pd.Series([1, "missing", None, 4], dtype=object),
            "payload": pd.Series([{"x": 1}, '{"x":1}', [1, 2], pd.NA], dtype=object),
        }
    )
    table, markers = _pandas_to_pyarrow_table(df, pa, container_markers=True)

    assert table.column_names == list(df.columns)
    assert pa.types.is_integer(table.schema.field("native").type)
    assert table["payload"].to_pylist() == ['{"x":1}', '{"x":1}', "[1,2]", None]
    assert markers == {2: [True, False, True, False]}
    assert df["payload"].iloc[0] == {"x": 1}
    assert df["mixed"].iloc[0] == 1


@pytest.mark.parametrize("max_rows", [None, 3, 5])
def test_container_flags_follow_arrow_batch_offsets_and_partial_final_batch(max_rows):
    from dataprof._inputs import _pandas_to_pyarrow_table

    df = pd.DataFrame(
        {
            "neighbor": pd.Series(
                pd.arrays.ArrowExtensionArray(pa.chunked_array([[1, 1], [1, 1], [1, 1]]))
            ),
            "payload": pd.Series(
                [{"x": 1}, '{"x":1}', {"x": 1}, '{"x":1}', '{"x":1}', {"x": 1}],
                dtype=object,
            ),
        }
    )
    assert [batch.num_rows for batch in _pandas_to_pyarrow_table(df, pa).to_batches()] == [2, 2, 2]

    report = dp.profile(df, max_rows=max_rows)

    rows = max_rows or 6
    assert report.rows == rows
    assert report["neighbor"].data_type == "integer"
    assert _uniqueness(report)["rows_checked"] == rows
    assert _uniqueness(report)["duplicate_rows"] == rows - 2


def test_projected_fallback_column_preserves_native_report_contract():
    df = pd.DataFrame(
        {
            "omitted": [1, 1, 1],
            "payload": pd.Series([{"x": 1}, '{"x":1}', {"x": 1}], dtype=object),
            "other": [True, False, True],
        }
    )
    report = dp.profile(df, max_rows=2, columns=["payload"])

    assert list(report) == ["payload"]
    assert report.rows == 2
    assert report.quality is not None
    assert report.quality.uniqueness is None
