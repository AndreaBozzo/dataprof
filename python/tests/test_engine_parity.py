"""Cross-engine parity suite (issue #363).

One canonical fixture, materialised into every input format dataprof supports,
profiled through every input path, asserting the resulting column profiles are
identical. Each engine is tested against its own hand-written expectations
elsewhere; this suite exists because an engine can be confidently, consistently
wrong on its own — the 0.9.0 nullable-Parquet and database-decoding bugs both
survived a full test suite and were only found by cross-engine disagreement.

Where engines *legitimately* differ, the difference is encoded as an explicit
expected exception with a comment, never by weakening the assertion.
"""

from __future__ import annotations

import csv
import json
from typing import Any

import pytest

try:
    import dataprof
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

# ── Canonical fixture ──
#
# Every case here reproduces a bug class that actually shipped:
#   amt       nullable float, nulls interleaved between real values (#358)
#   whole     integral floats — must infer float, not integer
#   n         integer containing a null — must stay integer, not widen
#   all_null  all-null column — Arrow NullArray has no validity buffer
#   big       integer beyond 2^53 — f64 round-trip loses precision
#   flag      boolean — must not render as "true"/"false" integers
#   null_str  literal string "NULL"
#   empty_str literal empty string
COLUMNS: dict[str, list[Any]] = {
    "amt": [100.0, None, 1.0, None, 2.0],
    "whole": [100.0, 1.0, 2.0, 3.0, 4.0],
    "n": [1, None, 2, 3, 4],
    "all_null": [None, None, None, None, None],
    "big": [9007199254740993, 9007199254740994, 3, 4, 5],
    "flag": [True, False, True, False, True],
    "null_str": ["NULL", "a", "b", "c", "d"],
    "empty_str": ["", "a", "b", "c", "d"],
}
N_ROWS = 5

# The full observable numeric/type surface of a ColumnProfile. Assert on all of
# it — a sampled field is how an engine stays consistently wrong.
FIELDS = ("data_type", "null_count", "unique_count", "min", "max", "mean", "std_dev")

# Explicit, justified exceptions: (engine, column, field) -> expected value.
# An entry here must explain why the difference is representational (the
# source library changes the data before dataprof sees it), not an engine bug.
EXPECTED_EXCEPTIONS: dict[tuple[str, str, str], Any] = {
    # pandas itself widens an int64 column containing NaN to float64, so by
    # the time dataprof profiles it the data really is float. Arrow, polars
    # and the text formats all preserve integer-with-null.
    ("pandas", "n", "data_type"): "float",
}

ENGINES = (
    "csv",
    "json",
    "jsonl",
    "dict",
    "rows",
    "parquet",
    "arrow",
    "pandas",
    "polars",
)


def fixture_rows() -> list[dict[str, Any]]:
    return [{name: values[i] for name, values in COLUMNS.items()} for i in range(N_ROWS)]


def _csv_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return repr(value)  # keeps the decimal point: "100.0", not "100"
    return str(value)


def build_report(engine: str, tmp_path):
    if engine == "dict":
        return dataprof.profile(COLUMNS)

    if engine == "rows":
        return dataprof.profile(fixture_rows())

    if engine == "csv":
        path = tmp_path / "fixture.csv"
        with open(path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(COLUMNS.keys())
            for row in fixture_rows():
                writer.writerow([_csv_cell(row[name]) for name in COLUMNS])
        return dataprof.profile(str(path))

    if engine == "json":
        path = tmp_path / "fixture.json"
        path.write_text(json.dumps(fixture_rows()), encoding="utf-8")
        return dataprof.profile(str(path))

    if engine == "jsonl":
        path = tmp_path / "fixture.jsonl"
        path.write_text("\n".join(json.dumps(row) for row in fixture_rows()), encoding="utf-8")
        return dataprof.profile(str(path))

    if engine == "parquet":
        pa = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")
        path = tmp_path / "fixture.parquet"
        pq.write_table(pa.table(COLUMNS), path)
        return dataprof.profile(str(path))

    if engine == "arrow":
        pa = pytest.importorskip("pyarrow")
        return dataprof.profile(pa.table(COLUMNS))

    if engine == "pandas":
        pd = pytest.importorskip("pandas")
        return dataprof.profile(pd.DataFrame(fixture_rows()))

    if engine == "polars":
        pl = pytest.importorskip("polars")
        return dataprof.profile(pl.DataFrame(COLUMNS))

    raise AssertionError(f"unknown engine {engine}")


def field_value(report, column: str, field: str) -> Any:
    return getattr(report[column], field)


def assert_profiles_match(engine: str, report, reference, exceptions) -> None:
    __tracebackhide__ = True
    assert report.rows == N_ROWS, f"{engine}: expected {N_ROWS} rows, got {report.rows}"
    mismatches = []
    for column in COLUMNS:
        for field in FIELDS:
            actual = field_value(report, column, field)
            expected = exceptions.get(
                (engine, column, field), field_value(reference, column, field)
            )
            if isinstance(expected, float) and isinstance(actual, float):
                matches = actual == pytest.approx(expected, rel=1e-9)
            else:
                matches = actual == expected
            if not matches:
                mismatches.append(f"  {column}.{field}: {engine}={actual!r} expected={expected!r}")
    assert not mismatches, f"{engine} disagrees with the reference profile on:\n" + "\n".join(
        mismatches
    )


@pytest.fixture(scope="module")
def reference():
    # dict-of-columns feeds the core directly with no file format or foreign
    # library in between, so it is the least-mediated path we have.
    return dataprof.profile(COLUMNS)


@pytest.mark.parametrize("engine", ENGINES)
def test_engine_parity(engine, reference, tmp_path):
    report = build_report(engine, tmp_path)
    assert_profiles_match(engine, report, reference, EXPECTED_EXCEPTIONS)


# ── High-cardinality regression (issue #386) ──
#
# The columnar (Arrow) engine used to stop counting distinct values at an
# internal cap of 1,000 and expose that cap as the exact unique_count, so a
# high-cardinality column reported 1,000 distinct and its quality score
# collapsed ~20 points below the streaming engines. Every file engine must now
# agree that a fully-distinct column has close to N distinct values.


@pytest.mark.parametrize("engine", ["auto", "columnar", "incremental"])
def test_high_cardinality_unique_count_not_capped(engine, tmp_path):
    n_rows = 50_000
    path = tmp_path / "high_card.csv"
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["id"])
        for value in range(n_rows):
            writer.writerow([value])

    report = dataprof.profile(str(path), engine=engine)
    unique = report["id"].unique_count

    assert unique is not None, f"{engine}: unique_count missing"
    assert unique != 1000, f"{engine}: unique_count frozen at the old hard cap"
    # HLL carries ~1% relative error; well within 5% of the true distinct count.
    assert abs(unique - n_rows) / n_rows < 0.05, (
        f"{engine}: {n_rows} distinct ids reported as {unique}"
    )


# ── SQLite arm (requires --features python-async,database,sqlite) ──


def test_sqlite_parity(tmp_path):
    import asyncio
    import sqlite3

    try:
        from dataprof._dataprof import analyze_database_async
    except ImportError:
        pytest.skip("database features not compiled")

    db_path = tmp_path / "fixture.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE fixture ("
        "amt REAL, whole REAL, n INTEGER, all_null TEXT,"
        "big INTEGER, flag BOOLEAN, null_str TEXT, empty_str TEXT)"
    )
    conn.executemany(
        "INSERT INTO fixture VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [tuple(row[name] for name in COLUMNS) for row in fixture_rows()],
    )
    conn.commit()
    conn.close()

    async def _run():
        return await analyze_database_async(str(db_path), "SELECT * FROM fixture")

    report = asyncio.run(_run())
    # The raw report exposes column_profiles as a list, not a mapping.
    profiles = {profile.name: profile for profile in report.column_profiles}

    # SQLite has no boolean type: sqlite3 stores True/False as INTEGER 1/0, so
    # the database really contains integers. The reference for this arm is the
    # same fixture as SQLite sees it, not a weakened assertion.
    sqlite_visible = dict(COLUMNS)
    sqlite_visible["flag"] = [int(value) for value in COLUMNS["flag"]]
    reference = dataprof.profile(sqlite_visible)

    assert report.rows_processed == N_ROWS
    mismatches = []
    for column in COLUMNS:
        for field in FIELDS:
            actual = getattr(profiles[column], field)
            expected = getattr(reference[column], field)
            if isinstance(expected, float) and isinstance(actual, float):
                matches = actual == pytest.approx(expected, rel=1e-9)
            else:
                matches = actual == expected
            if not matches:
                mismatches.append(f"  {column}.{field}: sqlite={actual!r} expected={expected!r}")
    assert not mismatches, "sqlite disagrees with the reference profile on:\n" + "\n".join(
        mismatches
    )
