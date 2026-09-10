"""Cross-engine parity suite (issue #363).

One canonical fixture, materialised into every input format dataprof supports,
profiled through every input path, asserting the serialized column profiles are
identical (#547). Raw floats retain full precision and are additionally checked
with explicit relative/absolute tolerances. Each engine has hand-written expectations
elsewhere; this suite exists because an engine can be confidently, consistently
wrong on its own — the 0.9.0 nullable-Parquet and database-decoding bugs both
survived a full test suite and were only found by cross-engine disagreement.

Where engines *legitimately* differ, the difference is encoded as an explicit
expected exception with a comment, never by weakening the assertion.
"""

from __future__ import annotations

import csv
import io
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

# Raw-access diagnostics supplement exact comparisons of every serialized field.
# These tolerances accommodate floating-point accumulation order; they do not
# apply to the serialized equality contract.
RAW_REL_TOL = 1e-9
RAW_ABS_TOL = 1e-12
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

# Same mechanism for the quality block: (engine, dimension, field) -> value.
QUALITY_EXCEPTIONS: dict[tuple[str, str, str], Any] = {
    # Downstream of the `n` widening above: four more values are numeric once
    # pandas has made them floats, so they enter the precision check. The
    # consistency percentage is unaffected, which is the point of pinning the
    # count rather than skipping the dimension.
    ("pandas", "precision", "numeric_values_checked"): 12,
}

ENGINES = (
    "csv",
    "csv.incremental",
    "csv.columnar",
    "csv.bytes",
    "csv.buffer",
    "json",
    "jsonl",
    "dict",
    "rows",
    "parquet",
    "parquet.bytes",
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

    if engine == "csv" or engine.startswith("csv."):
        path = tmp_path / "fixture.csv"
        with open(path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(COLUMNS.keys())
            for row in fixture_rows():
                writer.writerow([_csv_cell(row[name]) for name in COLUMNS])
        if engine == "csv.bytes":
            return dataprof.profile(path.read_bytes(), format="csv")
        if engine == "csv.buffer":
            return dataprof.profile(io.BytesIO(path.read_bytes()), format="csv")
        selected = engine.partition(".")[2] or "auto"
        return dataprof.profile(str(path), engine=selected)

    if engine == "json":
        path = tmp_path / "fixture.json"
        path.write_text(json.dumps(fixture_rows()), encoding="utf-8")
        return dataprof.profile(str(path))

    if engine == "jsonl":
        path = tmp_path / "fixture.jsonl"
        path.write_text("\n".join(json.dumps(row) for row in fixture_rows()), encoding="utf-8")
        return dataprof.profile(str(path))

    if engine in ("parquet", "parquet.bytes"):
        pa = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")
        path = tmp_path / "fixture.parquet"
        pq.write_table(pa.table(COLUMNS), path)
        if engine == "parquet.bytes":
            return dataprof.profile(path.read_bytes(), format="parquet")
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
    """Compare complete serialized columns exactly, then diagnose raw floats."""
    __tracebackhide__ = True
    assert report.rows == N_ROWS, f"{engine}: expected {N_ROWS} rows, got {report.rows}"
    expected_columns = reference.to_dict()["columns"]
    for column in expected_columns:
        for (exception_engine, name, field), value in exceptions.items():
            if exception_engine == engine and name == column["name"]:
                assert field in column, f"exception names an unknown serialized field: {field}"
                column[field] = value
    # No approximation or post-export re-rounding here: consumers see these
    # exact numbers, including every optional statistic and absence marker.
    assert report.to_dict()["columns"] == expected_columns, engine
    assert json.loads(report.to_json())["columns"] == expected_columns, engine
    # Quality scores are rounded metrics, not execution provenance, so they are
    # on the contract surface too. Left out, an engine could agree on every
    # column and still report a different overall score.
    expected_quality = reference.to_dict()["quality"]
    for (exception_engine, dimension, field), value in QUALITY_EXCEPTIONS.items():
        if exception_engine == engine:
            assert dimension in expected_quality, f"unknown quality dimension: {dimension}"
            assert field in expected_quality[dimension], f"unknown quality field: {field}"
            expected_quality[dimension][field] = value
    assert report.to_dict()["quality"] == expected_quality, engine
    mismatches = []
    for column in COLUMNS:
        for field in FIELDS:
            actual = field_value(report, column, field)
            expected = exceptions.get(
                (engine, column, field), field_value(reference, column, field)
            )
            if isinstance(expected, float) and isinstance(actual, float):
                matches = actual == pytest.approx(expected, rel=RAW_REL_TOL, abs=RAW_ABS_TOL)
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


@pytest.mark.parametrize("engine", ["auto", "incremental", "columnar"])
def test_serialized_parity_preserves_full_precision_accessors(engine, tmp_path):
    """Characterize the contract: serialized equality, native precision intact.

    This pins the two-surface behaviour the contract names; it is not a
    regression guard, and it passes on the code before #547 because nothing
    here was broken -- what was missing was the statement of which surface
    equality is defined on. The `to_llm_context()` assertion covers only the
    derived output this fixture reaches; `_dominant_pattern` still thresholds
    on native confidence and is tracked separately.
    """
    path = tmp_path / "fractional.csv"
    path.write_text("amount,label\n0,a\n0,東京\n1,café\n", encoding="utf-8")
    report = dataprof.profile(path, engine=engine)
    reference = dataprof.profile(path, engine="incremental")
    document = report.to_dict()
    assert document["columns"] == reference.to_dict()["columns"]
    assert document["quality"] == reference.to_dict()["quality"]
    assert document["columns"][0]["stats"]["mean"] == 0.3333
    assert report["amount"].mean == pytest.approx(1 / 3, rel=RAW_REL_TOL, abs=RAW_ABS_TOL)
    assert report["amount"].mean != document["columns"][0]["stats"]["mean"]
    # Loading retains the serialized precision rather than reconstructing the
    # producer's full-precision float. Derived agent output must still agree.
    restored = dataprof.ProfileReport.from_dict(document)
    assert restored["amount"].mean == 0.3333
    assert restored.to_dict() == document
    assert restored.to_llm_context() == report.to_llm_context()


# ── Column order (issue #465) ──
#
# Column order is part of the report, not an accident of the parser: a format
# conversion must not reshuffle it. The fixture keys above are deliberately
# non-alphabetical (sorting moves all_null to the front and whole to the back),
# so the JSON paths that used to emit object keys alphabetically fail here.


@pytest.mark.parametrize("engine", ENGINES)
def test_engine_column_order_follows_source(engine, tmp_path):
    report = build_report(engine, tmp_path)
    assert list(report) == list(COLUMNS)


@pytest.mark.parametrize("engine", ["auto", "incremental", "columnar"])
def test_whitespace_padded_numeric_stats_match_inference(engine, tmp_path):
    path = tmp_path / "padded_numeric.csv"
    path.write_bytes(b"name,amount\nAlice, 1 \nBob, 2 \n")

    amount = dataprof.profile(str(path), engine=engine)["amount"]

    assert amount.data_type == "integer", engine
    assert amount.invalid_count == 0, engine
    assert (amount.min, amount.max, amount.mean) == pytest.approx((1.0, 2.0, 1.5)), engine


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


# ── Distinct-count provenance (issue #383) ──
#
# unique_count must not present an approximate HLL estimate as an exact integer.
# Small columns are exact across every engine; a high-cardinality column past the
# estimator threshold is flagged approximate. The flag also survives to_dict().


@pytest.mark.parametrize("engine", ["auto", "columnar", "incremental"])
def test_small_distinct_count_marked_exact(engine, tmp_path):
    path = tmp_path / "small.csv"
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["category"])
        for value in ["a", "b", "c", "a", "b"]:
            writer.writerow([value])

    report = dataprof.profile(str(path), engine=engine)
    col = report["category"]
    assert col.unique_count == 3
    assert col.unique_count_is_approximate is False, (
        f"{engine}: small exact count must be flagged exact, not "
        f"{col.unique_count_is_approximate!r}"
    )
    assert report.to_dict()["columns"][0]["unique_count_is_approximate"] is False


@pytest.mark.parametrize("engine", ["auto", "columnar", "incremental"])
def test_high_cardinality_distinct_count_marked_approximate(engine, tmp_path):
    # 20k comfortably clears the 10k estimator threshold (matches the Rust
    # parity test) without the IO of a larger file across three engines.
    n_rows = 20_000
    path = tmp_path / "high_card.csv"
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["id"])
        for value in range(n_rows):
            writer.writerow([value])

    report = dataprof.profile(str(path), engine=engine)
    col = report["id"]
    assert col.unique_count_is_approximate is True, (
        f"{engine}: HLL-estimated count must be flagged approximate, not "
        f"{col.unique_count_is_approximate!r}"
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

    # SQLite has no boolean type: sqlite3 stores True/False as INTEGER 1/0, so
    # the database really contains integers. The reference for this arm is the
    # same fixture as SQLite sees it, not a weakened assertion.
    sqlite_visible = dict(COLUMNS)
    sqlite_visible["flag"] = [int(value) for value in COLUMNS["flag"]]
    reference = dataprof.profile(sqlite_visible)

    assert_profiles_match("sqlite", dataprof.ProfileReport(report), reference, {})


# ── Duplicate-row detection with nulls (issue #417) ──
#
# The columnar engine used to skip duplicate-row detection whenever any column
# contained nulls: per-column sample reservoirs lose row alignment, the
# sample-based scan correctly refused to run, and — with no full-stream
# tracker — the uniqueness dimension silently dropped its duplicate component.
# Same file, different overall quality score depending on the engine.


def _dup_rows_csv(tmp_path):
    path = tmp_path / "dups_with_nulls.csv"
    path.write_text(
        "id,name,value,city\n"
        "1,Alice,10.5,Rome\n"
        "2,,20.0,Milan\n"
        "2,,20.0,Milan\n"  # duplicate row containing a null
        "3,Bob,,Naples\n"
        "4,Alice,10.5,\n"
        "1,Alice,10.5,Rome\n",  # duplicate of the first row
        encoding="utf-8",
    )
    return path


def test_duplicate_rows_with_nulls_agree_across_engines(tmp_path):
    path = _dup_rows_csv(tmp_path)
    reports = {
        engine: dataprof.profile(str(path), engine=engine) for engine in ("incremental", "columnar")
    }

    facts = {}
    for engine, report in reports.items():
        uniqueness = report.to_dict()["quality"]["uniqueness"]
        facts[engine] = (
            uniqueness["rows_checked"],
            uniqueness["duplicate_rows"],
            uniqueness["key_uniqueness"],
        )
        assert uniqueness["rows_checked"] == 6, f"{engine} must scan every row"
        assert uniqueness["duplicate_rows"] == 2, f"{engine} must find both duplicates"

    assert facts["incremental"] == facts["columnar"]
    assert reports["incremental"].quality_score == pytest.approx(
        reports["columnar"].quality_score, abs=0.01
    ), "overall quality must not depend on the engine"


def test_duplicate_rows_tracked_for_dataframe_and_arrow_inputs(tmp_path):
    # The DataFrame/Arrow paths share RecordBatchAnalyzer; duplicates must be
    # assessed there too, not silently skipped when a column has nulls.
    pa = pytest.importorskip("pyarrow")
    data = {
        "id": [1, 2, 2, 3],
        "name": ["Alice", None, None, "Bob"],
    }
    report = dataprof.profile(pa.table(data))
    uniqueness = report.to_dict()["quality"]["uniqueness"]
    assert uniqueness["rows_checked"] == 4
    assert uniqueness["duplicate_rows"] == 1


def test_jsonl_duplicates_survive_a_key_discovered_mid_stream(tmp_path):
    # JSONL has no header, so a key can appear at any point and widen the
    # schema. Two identical records must still count as one duplicate whether
    # or not a new key was discovered between them, and the count must match
    # the same data written with the key present everywhere.
    late = tmp_path / "late.jsonl"
    late.write_text(
        '{"a": "x"}\n{"a": "x", "b": "y"}\n{"a": "x"}\n',
        encoding="utf-8",
    )
    explicit = tmp_path / "explicit.jsonl"
    explicit.write_text(
        '{"a": "x", "b": null}\n{"a": "x", "b": "y"}\n{"a": "x", "b": null}\n',
        encoding="utf-8",
    )

    late_uniqueness = dataprof.profile(str(late)).to_dict()["quality"]["uniqueness"]
    explicit_uniqueness = dataprof.profile(str(explicit)).to_dict()["quality"]["uniqueness"]

    assert late_uniqueness["rows_checked"] == 3
    assert late_uniqueness["duplicate_rows"] == 1
    assert late_uniqueness == explicit_uniqueness


def test_duplicate_column_names_rejected_across_all_inputs(tmp_path):
    # The same duplicate-header source must be rejected identically regardless of
    # transport, so no path can silently merge or shadow a column (#381).
    path = tmp_path / "dup.csv"
    path.write_text("x,x,y\n1,2,a\n3,4,b\n")

    inputs = [
        ("csv file", lambda: dataprof.profile(str(path))),
        ("csv bytes", lambda: dataprof.profile(b"x,x,y\n1,2,a\n3,4,b\n", format="csv")),
        # dict/list-of-dicts collide only after str() normalization (1 vs "1").
        ("list-of-dicts", lambda: dataprof.profile([{1: "a", "1": "b"}])),
    ]
    pa = pytest.importorskip("pyarrow", reason="pyarrow optional")
    inputs.append(
        (
            "arrow",
            lambda: dataprof.profile(
                pa.table(
                    [pa.array([1, 3]), pa.array([2, 4])],
                    schema=pa.schema([("x", pa.int64()), ("x", pa.int64())]),
                )
            ),
        )
    )

    for label, call in inputs:
        with pytest.raises(ValueError):
            call()


def test_rectangular_source_has_one_profile_per_column(tmp_path):
    # The invariant duplicate-rejection protects: columns == len(profiles) and
    # every column's total_count equals the row count.
    path = tmp_path / "rect.csv"
    path.write_text("a,b,c\n1,2,3\n4,5,6\n")
    for engine in ("auto", "incremental", "columnar"):
        report = dataprof.profile(str(path), engine=engine)
        assert report.columns == len(list(report.column_profiles)) == 3, engine
        for col in report.column_profiles:
            assert report[col].total_count == report.rows, f"{engine} {col}"


@pytest.mark.parametrize("engine", ["auto", "incremental", "columnar"])
def test_non_finite_csv_tokens_keep_numeric_type(engine, tmp_path):
    path = tmp_path / "non_finite.csv"
    path.write_text("x\n1.0\nInfinity\n-inf\n2.0\n")

    column = dataprof.profile(path, engine=engine)["x"]
    assert column.data_type == "float"
    assert column.invalid_count == 2
    assert column.min == 1.0
    assert column.max == 2.0


@pytest.mark.parametrize("engine", ["auto", "incremental", "columnar"])
def test_all_non_finite_csv_tokens_keep_numeric_type(engine, tmp_path):
    path = tmp_path / "all_non_finite.csv"
    path.write_text("x\nInfinity\n-inf\n")

    column = dataprof.profile(path, engine=engine)["x"]
    assert column.data_type == "float"
    assert column.invalid_count == 2


@pytest.mark.parametrize("engine", ["auto", "incremental", "columnar"])
def test_unsigned_values_beyond_i64_keep_integer_type(engine, tmp_path):
    path = tmp_path / "unsigned.csv"
    path.write_text(f"x\n{2**64 - 1}\n{2**64 - 2}\n")

    report = dataprof.profile(path, engine=engine)
    column = report["x"]
    assert column.data_type == "integer"
    assert report.quality is not None
    assert report.quality.consistency is not None
    assert report.quality.consistency["data_type_consistency"] == 100.0
