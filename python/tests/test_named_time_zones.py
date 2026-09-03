"""A timestamp column in a named time zone profiles, on every path.

`arrow` was declared with `features = ["ffi"]` alone. Without its `chrono-tz`
feature, arrow resolves offset-based zones only, so `ArrayFormatter::try_new`
on a `Timestamp(_, Some("UTC"))` array returned

    Invalid timezone "UTC": only offset based timezones supported without
    chrono-tz feature

and the error propagated out of the analyzer and failed the whole report. Not a
wrong number: no report at all.

`UTC` is the spelling pyarrow, pandas and polars all produce by default, and it
is what `pq.write_table` stores in an ordinary Parquet file, so this hit the
normal shape of a timestamped dataset rather than an exotic one. Only the
`+00:00` spelling and naive timestamps worked.

The DST half of the fix -- that `Europe/Rome` renders +01:00 in January and
+02:00 in June rather than one fixed offset -- is pinned in Rust, in
`record_batch_analyzer::tests::named_time_zones_are_resolved_with_their_dst_rules`.
It belongs there because the rendered wall-clock string reaches
`create_sample_columns` but not the Python report surface, so a test here could
only assert that nothing raised, which a fixed-offset workaround would also
satisfy while reporting the wrong hour for half the year.
"""

from __future__ import annotations

import datetime as dt

import dataprof
import pytest

pa = pytest.importorskip("pyarrow", reason="pyarrow is required for Arrow import tests")

INSTANTS = [dt.datetime(2021, 1, 15, 12, 0), dt.datetime(2021, 6, 15, 12, 0)]


def _table(tz):
    return pa.table({"seen_at": pa.array(INSTANTS, type=pa.timestamp("us", tz=tz))})


def _columns(source, name):
    return dataprof.profile(source, name=name).to_dict()["columns"]


@pytest.mark.parametrize("tz", ["UTC", "Europe/Rome", "America/New_York", "+00:00", None])
def test_a_timestamp_column_profiles_whatever_its_zone_is_called(tz):
    """Named zones, offset zones and naive timestamps all produce a report."""
    columns = _columns(_table(tz), f"tz_{tz}")

    assert len(columns) == 1
    assert columns[0]["name"] == "seen_at"
    assert columns[0]["data_type"] == "date"
    assert columns[0]["total_count"] == len(INSTANTS)
    assert columns[0]["null_count"] == 0


def test_every_path_agrees_on_a_utc_column(tmp_path):
    """The output contract, over the spelling that used to fail everywhere.

    A pyarrow Table, the Parquet file written from it, and the pandas and polars
    frames built from it are the same data by construction, so the whole column
    section has to agree.
    """
    pytest.importorskip("pandas", reason="pandas is required for pandas interop tests")
    pl = pytest.importorskip("polars", reason="polars is required for polars interop tests")
    pq = pytest.importorskip("pyarrow.parquet", reason="pyarrow.parquet is required")

    table = _table("UTC")
    path = tmp_path / "tz.parquet"
    pq.write_table(table, path)

    reference = _columns(table, "pyarrow")
    assert _columns(str(path), "parquet") == reference
    assert _columns(table.to_pandas(), "pandas") == reference
    assert _columns(pl.from_arrow(table), "polars") == reference


def test_a_named_zone_and_its_offset_spelling_describe_the_same_instants():
    """`UTC` and `+00:00` are two spellings of one zone, so the counts match.

    This is the pair that used to disagree in the sharpest possible way: one
    raised and the other profiled.
    """
    assert _columns(_table("UTC"), "named") == _columns(_table("+00:00"), "offset")


def test_a_null_timestamp_in_a_named_zone_is_counted_not_rendered():
    """Nulls must not be pushed through the formatter that used to fail."""
    array = pa.array([INSTANTS[0], None], type=pa.timestamp("us", tz="Europe/Rome"))
    column = _columns(pa.table({"seen_at": array}), "with_null")[0]

    assert column["total_count"] == 2
    assert column["null_count"] == 1
