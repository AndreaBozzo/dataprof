"""Pandas mixed-object fallback preserves container identity in row signatures."""

from __future__ import annotations

import dataprof as dp
import pytest

pd = pytest.importorskip("pandas", reason="pandas is required for pandas DataFrame tests")
pytest.importorskip("pyarrow", reason="pyarrow is required for Arrow import tests")


def _report(entrypoint: str, values: list[object]) -> dp.ProfileReport:
    df = pd.DataFrame({"c": pd.Series(values, dtype=object)})
    if entrypoint == "profile":
        return dp.profile(df)

    from dataprof.interop import profile_dataframe

    return dp.ProfileReport(profile_dataframe(df))


@pytest.mark.parametrize("entrypoint", ["profile", "interop"])
def test_container_and_json_spelling_are_distinct_rows(entrypoint: str):
    values = [{"x": 1}, '{"x":1}', [1, 2], "[1,2]"]

    report = _report(entrypoint, values)

    assert report.quality.uniqueness["duplicate_rows"] == 0


@pytest.mark.parametrize("entrypoint", ["profile", "interop"])
def test_equal_containers_and_equal_strings_still_duplicate(entrypoint: str):
    values = [{"x": 1}, {"x": 1}, '{"x":1}', '{"x":1}']

    report = _report(entrypoint, values)

    assert report.quality.uniqueness["duplicate_rows"] == 2


def test_arrow_compatible_container_column_keeps_native_path():
    from dataprof._pandas_mixed import _pandas_container_fallback_columns

    df = pd.DataFrame({"c": pd.Series([[1, 2], [3, 4]], dtype=object)})

    assert _pandas_container_fallback_columns(df) is None
