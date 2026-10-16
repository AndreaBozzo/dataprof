"""A pandas object column with mixed Python types must profile cleanly (#976).

When a pandas DataFrame contains object columns with mixed Python types
(such as numeric columns with placeholder strings, or mixed booleans/dates),
pyarrow cannot cast them into a single physical Arrow type and fails with
``ArrowInvalid`` or ``ArrowTypeError``.

Rather than aborting the entire profile, such columns are profiled from
their values' text (as ``list[dict]`` input does), with ``type_homogeneity``
recording the mix, while the other typed columns in the frame keep their
native Arrow-derived profiles unchanged.
"""

from __future__ import annotations

import datetime
import decimal

import pytest

import dataprof as dp

pd = pytest.importorskip("pandas", reason="pandas is required for pandas DataFrame tests")
pa = pytest.importorskip("pyarrow", reason="pyarrow is required for Arrow import tests")


def test_pandas_mixed_object_columns_profile_cleanly():
    """Verify each case from #976 profiles rather than raising raw Arrow exceptions."""
    cases = [
        # values in object column -> expected data_type, expected type_homogeneity
        (
            [1, "2", 3],
            "integer",
            {"boolean": 0, "date": 0, "numeric": 3, "text": 0},
        ),
        (
            [1.5, "n/a", 2.0],
            "float",
            {"boolean": 0, "date": 0, "numeric": 2, "text": 0},
        ),
        (
            [True, "yes", False],
            "string",
            {"boolean": 2, "date": 0, "numeric": 0, "text": 1},
        ),
        (
            [datetime.date(2024, 1, 1), "unknown", datetime.date(2024, 1, 2)],
            "string",
            {"boolean": 0, "date": 2, "numeric": 0, "text": 1},
        ),
        (
            ["a", None, "b"],
            "string",
            {"boolean": 0, "date": 0, "numeric": 0, "text": 2},
        ),
        (
            [1, decimal.Decimal("2.5"), 3],
            "float",
            {"boolean": 0, "date": 0, "numeric": 3, "text": 0},
        ),
    ]

    for values, expected_type, expected_homogeneity in cases:
        df = pd.DataFrame({"c": pd.Series(values, dtype=object)})
        report = dp.profile(df)
        assert report["c"].data_type == expected_type
        assert report["c"].type_homogeneity == expected_homogeneity


def test_frame_with_mixed_column_and_typed_columns_preserves_typed_profiles():
    """Typed columns keep their native Arrow-derived profiles alongside a mixed column."""
    df_clean = pd.DataFrame({
        "id": [1, 2, 3],
        "score": [95.5, 82.0, 77.5],
        "name": ["Alice", "Bob", "Charlie"],
    })
    clean_report = dp.profile(df_clean)

    df_mixed = pd.DataFrame({
        "id": [1, 2, 3],
        "score": [95.5, 82.0, 77.5],
        "name": ["Alice", "Bob", "Charlie"],
        "notes": pd.Series([1.5, "n/a", 2.0], dtype=object),
    })
    mixed_report = dp.profile(df_mixed)

    for col in ("id", "score", "name"):
        assert mixed_report[col].data_type == clean_report[col].data_type
        assert mixed_report[col].null_count == clean_report[col].null_count
        assert mixed_report[col].total_count == clean_report[col].total_count

    assert mixed_report["notes"].data_type == "float"
    assert mixed_report["notes"].type_homogeneity == {
        "boolean": 0,
        "date": 0,
        "numeric": 2,
        "text": 0,
    }


def test_unconvertible_column_raises_value_error_naming_column():
    """If a column genuinely cannot be converted, raise ValueError naming the column."""
    class Unconvertible:
        def __str__(self):
            raise RuntimeError("cannot stringify object")

    df = pd.DataFrame({"bad_column": [Unconvertible()]})
    with pytest.raises(ValueError, match="column 'bad_column'"):
        dp.profile(df)


def test_interop_profile_dataframe_mixed_columns():
    """Verify that lower-level interop.profile_dataframe also handles mixed columns."""
    from dataprof.interop import profile_dataframe

    df = pd.DataFrame({
        "c": pd.Series([1.5, "unknown", 2.0], dtype=object),
    })
    raw_report = profile_dataframe(df)
    report = dp.ProfileReport(raw_report)
    assert report["c"].data_type == "string"
    assert report["c"].type_homogeneity == {
        "boolean": 0,
        "date": 0,
        "numeric": 2,
        "text": 1,
    }
