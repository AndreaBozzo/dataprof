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

import dataprof as dp
import pytest

pd = pytest.importorskip("pandas", reason="pandas is required for pandas DataFrame tests")
pa = pytest.importorskip("pyarrow", reason="pyarrow is required for Arrow import tests")


def test_pandas_mixed_object_columns_profile_cleanly():
    """Match list-of-dicts profiles for each mixed-column case from #976."""
    cases = [
        [1, "2", 3],
        [1.5, "n/a", 2.0],
        [True, "yes", False],
        [datetime.date(2024, 1, 1), "unknown", datetime.date(2024, 1, 2)],
        ["a", None, "b"],
        [1, decimal.Decimal("2.5"), 3],
    ]

    for values in cases:
        df = pd.DataFrame({"c": pd.Series(values, dtype=object)})
        dataframe_column = dp.profile(df)["c"]
        records_column = dp.profile([{"c": value} for value in values])["c"]

        assert dataframe_column.data_type == records_column.data_type
        assert dataframe_column.type_homogeneity == records_column.type_homogeneity


def test_frame_with_mixed_column_and_typed_columns_preserves_typed_profiles():
    """Typed columns keep their native Arrow-derived profiles alongside a mixed column."""
    df_clean = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "score": [95.5, 82.0, 77.5],
            "name": ["Alice", "Bob", "Charlie"],
        }
    )
    clean_report = dp.profile(df_clean)

    df_mixed = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "score": [95.5, 82.0, 77.5],
            "name": ["Alice", "Bob", "Charlie"],
            "notes": pd.Series([1.5, "n/a", 2.0], dtype=object),
        }
    )
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

    df = pd.DataFrame(
        {
            "c": pd.Series([1.5, "unknown", 2.0], dtype=object),
        }
    )
    raw_report = profile_dataframe(df)
    report = dp.ProfileReport(raw_report)
    assert report["c"].data_type == "string"
    assert report["c"].type_homogeneity == {
        "boolean": 0,
        "date": 0,
        "numeric": 2,
        "text": 1,
    }


def test_pandas_multiindex_columns_with_mixed_objects():
    """Verify that DataFrames with tuple/MultiIndex columns and mixed objects profile."""
    df = pd.DataFrame(
        {
            ("metrics", "score"): pd.Series([1.5, "n/a", 2.0], dtype=object),
            ("metrics", "count"): [10, 20, 30],
        }
    )
    report = dp.profile(df)
    score_col = "('metrics', 'score')"
    assert report[score_col].data_type == "float"
    assert report[score_col].type_homogeneity == {
        "boolean": 0,
        "date": 0,
        "numeric": 2,
        "text": 0,
    }


def test_pandas_duplicate_columns_raises_clear_error():
    """Duplicate column labels fail cleanly and are not misclassified as mixed objects."""
    df = pd.DataFrame([[1, 2]], columns=["a", "a"])
    with pytest.raises(Exception) as exc_info:
        dp.profile(df)
    assert "Duplicate column names found" in str(exc_info.value)


def test_pandas_mixed_columns_preserve_missing_values_as_null():
    """Verify that pd.NA and pd.NaT in mixed object columns become Arrow nulls,
    not string literals.
    """
    from dataprof._inputs import _pandas_to_pyarrow_table

    df = pd.DataFrame(
        {
            "id": [10, 20, 30],
            "mixed_na": pd.Series([1, "two", pd.NA], dtype=object),
            "mixed_nat": pd.Series([1, "two", pd.NaT], dtype=object),
        }
    )
    original = df.copy(deep=True)

    tbl = _pandas_to_pyarrow_table(df, pa)
    assert df.equals(original)
    assert tbl["mixed_na"].to_pylist() == ["1", "two", None]
    assert tbl["mixed_na"].null_count == 1
    assert tbl["mixed_nat"].to_pylist() == ["1", "two", None]
    assert tbl["mixed_nat"].null_count == 1

    # Neighboring typed column retains native Arrow integer type and values
    assert pa.types.is_integer(tbl["id"].type)
    assert tbl["id"].to_pylist() == [10, 20, 30]

    report = dp.profile(df)
    assert report["mixed_na"].null_count == 1
    assert report["mixed_na"].total_count == 3
    assert report["mixed_na"].type_homogeneity == {
        "boolean": 0,
        "date": 0,
        "numeric": 1,
        "text": 1,
    }
    assert report["mixed_nat"].null_count == 1
    assert report["mixed_nat"].total_count == 3
    assert report["id"].data_type == "integer"
    assert report["id"].null_count == 0


def test_pandas_mixed_columns_do_not_misclassify_containers_as_null():
    """Verify container objects like dict and list in mixed columns are not
    misclassified as null.
    """
    from dataprof._inputs import _pandas_to_pyarrow_table

    df = pd.DataFrame(
        {
            "mixed_containers": pd.Series([1, "two", {"key": "val"}, [1, 2]], dtype=object),
        }
    )

    tbl = _pandas_to_pyarrow_table(df, pa)
    assert tbl["mixed_containers"].to_pylist() == ["1", "two", '{"key":"val"}', "[1,2]"]
    assert tbl["mixed_containers"].null_count == 0
