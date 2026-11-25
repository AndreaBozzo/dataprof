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
import re
from decimal import Decimal
from typing import Any

import pytest
from _provenance import without_native_type

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

# Statistics an engine leaves out for the same representational reason:
# (engine, column) -> stats keys absent there.
ABSENT_STATS: dict[tuple[str, str], tuple[str, ...]] = {
    # Downstream of the `n` widening: a float column has no exact order
    # statistics, which integer columns carry (#1038).
    ("pandas", "n"): ("exact",),
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
    "arrow.stream",
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

    if engine in ("arrow", "arrow.stream"):
        pa = pytest.importorskip("pyarrow")
        table = pa.table(COLUMNS)
        return dataprof.profile(
            table.to_reader(max_chunksize=2) if engine == "arrow.stream" else table
        )

    if engine == "pandas":
        pd = pytest.importorskip("pandas")
        return dataprof.profile(pd.DataFrame(fixture_rows()))

    if engine == "polars":
        pl = pytest.importorskip("polars")
        return dataprof.profile(pl.DataFrame(COLUMNS))

    raise AssertionError(f"unknown engine {engine}")


@pytest.mark.parametrize("engine", ENGINES)
def test_column_fingerprints_match_across_routes(engine, tmp_path):
    report = build_report(engine, tmp_path)
    reference = dataprof.profile(COLUMNS)
    actual = {name: report[name].fingerprint for name in COLUMNS}
    assert all(
        value is not None and re.fullmatch(r"[0-9a-f]{32}", value) for value in actual.values()
    )
    assert actual == {name: reference[name].fingerprint for name in COLUMNS}
    # This fixture's all-null column has five cells. Pin the multiplication
    # independently of either profiler route, including modulo wraparound.
    assert actual["all_null"] == f"{(0xA6CD5E9392000F6AC44BDFF4074EECDB * 5) % (1 << 128):032x}"


def test_column_fingerprint_uses_values_multiplicity_and_numeric_scale(tmp_path):
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    # This is the sum of the pinned hashes of 1, 1.5, 1.5 and null.
    expected = "b145fdfd90a4bc5933a166681c5adb88"
    csv_path = tmp_path / "values.csv"
    csv_path.write_bytes(b"v\n1.50\n1\n1.50\nNULL\n")
    for engine in ("auto", "incremental", "columnar"):
        assert dataprof.profile(csv_path, engine=engine)["v"].fingerprint == expected

    document = b'[{"v": null}, {"v": 1.5}, {"v": 1.00}, {"v": 1.5}]'
    assert dataprof.profile(document, format="json")["v"].fingerprint == expected
    assert (
        dataprof.profile({"v": [1, Decimal("1.5000"), None, Decimal("1.5")]})["v"].fingerprint
        == expected
    )
    changed = dataprof.profile({"v": [1.0, 1.5, 1.6, None]})["v"].fingerprint
    without_null = dataprof.profile({"v": [1.0, 1.5, 1.5]})["v"].fingerprint
    assert changed is not None and changed != expected
    assert without_null is not None and without_null != expected

    for scale in (1, 4):
        table = pa.table(
            {
                "v": pa.array(
                    [Decimal("1.5"), None, Decimal("1"), Decimal("1.5")],
                    type=pa.decimal128(8, scale),
                )
            }
        )
        parquet_path = tmp_path / f"decimal-{scale}.parquet"
        pq.write_table(table, parquet_path, row_group_size=2)
        for source in (table, table.to_reader(max_chunksize=1), parquet_path):
            assert dataprof.profile(source)["v"].fingerprint == expected
        assert (
            dataprof.profile(parquet_path.read_bytes(), format="parquet")["v"].fingerprint
            == expected
        )


@pytest.mark.parametrize(
    ("fmt", "data"),
    [
        ("csv", b"v,k\nNULL,a\n1.50,b\n1,c\n"),
        ("json", b'[{"k":"a"},{"v":1.5,"k":"b"},{"v":1.0,"k":"c"}]'),
        ("jsonl", b'{"k":"a"}\n{"v":1.5,"k":"b"}\n{"v":1.0,"k":"c"}\n'),
    ],
)
def test_async_column_fingerprint_parity(tmp_path, fmt, data):
    import asyncio

    from dataprof.asyncio import profile_bytes, profile_file

    if not dataprof.capabilities().async_streaming:
        pytest.skip("async streaming not compiled")
    path = tmp_path / f"input.{fmt}"
    path.write_bytes(data)
    expected = dataprof.profile({"v": [1.5, None, 1]})["v"].fingerprint
    assert expected is not None
    assert dataprof.profile(path)["v"].fingerprint == expected
    assert asyncio.run(profile_file(path, chunk_size=1))["v"].fingerprint == expected
    assert asyncio.run(profile_bytes(data, format=fmt, chunk_size=1))["v"].fingerprint == expected


def test_padded_float_column_fingerprint_matches_arrow(tmp_path):
    pa = pytest.importorskip("pyarrow")
    path = tmp_path / "padded.csv"
    path.write_bytes(b"v\n4.9406564584124654e-324\n1.4999833007740245e-320\n0.10000000000000001\n")
    expected = dataprof.profile(pa.table({"v": [5e-324, 1.5e-320, 0.1]}))["v"].fingerprint
    assert expected is not None
    for engine in ("auto", "incremental", "columnar"):
        assert dataprof.profile(path, engine=engine)["v"].fingerprint == expected


def test_partial_column_fingerprint_is_absent(tmp_path):
    path = tmp_path / "values.csv"
    path.write_bytes(b"v\n1\n2\n3\n4\n")
    for engine in ("auto", "incremental", "columnar"):
        assert dataprof.profile(path, engine=engine, max_rows=4)["v"].fingerprint is not None
        assert dataprof.profile(path, engine=engine, max_rows=2)["v"].fingerprint is None
    assert (
        dataprof.profile(
            path, engine="incremental", stop_condition=dataprof.StopCondition.max_rows(2)
        )["v"].fingerprint
        is None
    )
    assert (
        dataprof.profile(path, sampling=dataprof.SamplingStrategy.reservoir(2))["v"].fingerprint
        is None
    )
    assert dataprof.profile({"v": [1, 2]}, max_rows=1)["v"].fingerprint is None
    skipped = dataprof.profile(
        b'{"v": 1}\ninvalid\n{"v": 2}\n', format="jsonl", jsonl_on_error="skip"
    )
    assert skipped.error_count == 1
    assert skipped["v"].fingerprint is None


def test_slash_date_column_fingerprint_uses_the_whole_column(tmp_path):
    # The second row supplies the first evidence of order, in a different
    # batch from the ambiguous date and datetime.
    pa = pytest.importorskip("pyarrow")
    month_first = ["01/02/2024 12:30:00", "12/31/2024", "03/04/2024"]
    iso = ["2024-01-02T12:30:00", "2024-12-31", "2024-03-04"]
    expected = dataprof.profile({"v": iso})["v"].fingerprint
    assert expected is not None
    assert dataprof.profile({"v": month_first})["v"].fingerprint == expected
    assert (
        dataprof.profile(pa.table({"v": month_first}).to_reader(max_chunksize=1))["v"].fingerprint
        == expected
    )
    path = tmp_path / "slash.csv"
    path.write_text("v\n" + "\n".join(month_first) + "\n", encoding="utf-8")
    assert dataprof.profile(path, metrics=["schema"])["v"].fingerprint == expected
    mixed = [*month_first, "31/12/2024"]
    assert dataprof.profile({"v": mixed}, metrics=["schema"])["v"].fingerprint is None
    assert (
        dataprof.profile(pa.table({"v": mixed}).to_reader(max_chunksize=1))["v"].fingerprint is None
    )


def test_unavailable_column_fingerprints_and_empty_columns(tmp_path):
    pa = pytest.importorskip("pyarrow")
    zero = "0" * 32
    for source in ({"v": []}, pa.table({"v": pa.array([], type=pa.int64())})):
        assert dataprof.profile(source)["v"].fingerprint == zero
    header = tmp_path / "empty.csv"
    header.write_bytes(b"v\n")
    for engine in ("auto", "incremental", "columnar"):
        assert dataprof.profile(header, engine=engine)["v"].fingerprint == zero

    wide = "1234567890123456789012345678901234567891"
    assert dataprof.profile({"v": [wide]})["v"].fingerprint is None
    assert (
        dataprof.profile(pa.table({"v": pa.array([Decimal(wide)], type=pa.decimal256(40, 0))}))[
            "v"
        ].fingerprint
        is None
    )
    assert dataprof.profile({"v": [[1], None]})["v"].fingerprint is None
    mixed = tmp_path / "mixed.json"
    mixed.write_text('[{"v": [1]}, {"v": "scalar"}]', encoding="utf-8")
    assert dataprof.profile(mixed)["v"].fingerprint is None
    assert dataprof.profile({"v": [[1], "scalar"]})["v"].fingerprint is None
    assert (
        dataprof.profile(pa.table({"v": pa.array([], type=pa.list_(pa.int64()))}))["v"].fingerprint
        is None
    )
    assert dataprof.profile(pa.table({"v": [b"\0\xff"]}))["v"].fingerprint == (
        "c22451481e79b5755e1b71ed05857c1d"
    )


def test_column_fingerprint_roundtrips_and_preserves_absence():
    report = dataprof.profile({"v": [1.5, None], "nested": [[1], None]})
    expected = report["v"].fingerprint
    assert expected is not None
    documents = (report.to_dict(), json.loads(report.to_json()))
    for document in documents:
        columns = document.get("columns", document.get("column_profiles"))
        assert columns[0]["fingerprint"] == expected
        assert "fingerprint" not in columns[1]
        for restored in (
            dataprof.ProfileReport.from_dict(document),
            dataprof.ProfileReport.from_json(json.dumps(document)),
        ):
            assert restored["v"].fingerprint == expected
            assert restored["nested"].fingerprint is None
            assert restored.to_dict()["columns"][0]["fingerprint"] == expected
        del columns[0]["fingerprint"]
        legacy = dataprof.ProfileReport.from_dict(document)
        assert legacy["v"].fingerprint is None
        assert "fingerprint" not in legacy.to_dict()["columns"][0]
        columns[0]["fingerprint"] = "A" * 32
        with pytest.raises(ValueError, match="fingerprint"):
            dataprof.ProfileReport.from_dict(document)


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
        for key in ABSENT_STATS.get((engine, column["name"]), ()):
            assert key in column["stats"], f"absence names an unknown statistic: {key}"
            del column["stats"][key]
    # No approximation or post-export re-rounding here: consumers see these
    # exact numbers, including every optional statistic and absence marker.
    # Each route declares its own column types, or none (#906): provenance.
    assert without_native_type(report.to_dict()["columns"]) == without_native_type(
        expected_columns
    ), engine
    # Compare the canonical documents directly too, without re-rounding via
    # the convenience projection. Source/execution provenance is separate.
    canonical_columns = json.loads(reference.to_json())["column_profiles"]
    for column in canonical_columns:
        for (exception_engine, name, field), value in exceptions.items():
            if exception_engine == engine and name == column["name"]:
                column[field] = value.capitalize() if field == "data_type" else value
        for key in ABSENT_STATS.get((engine, column["name"]), ()):
            del column["stats"]["Numeric"][key]
    assert without_native_type(
        json.loads(report.to_json())["column_profiles"]
    ) == without_native_type(canonical_columns), engine
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
def test_every_input_path_records_the_same_metric_semantics(engine, tmp_path):
    """Provenance, so identical on every path and in both documents (#675)."""
    report = build_report(engine, tmp_path)
    expected = {
        "text_length_unit": "unicode_scalar",
        "null_tokens": "common_markers",
        "pattern_match_count": "validated",
        "shape_moments": "adjusted_sample",
        "statistic_rounding": "four_decimals_twelve_significant",
        "range_rule_count": "violating_values",
        "key_uniqueness_units": "non_null_values",
        "outlier_units": "tested_values",
        "key_column_source": "declared",
        "accuracy_comparison": "exact_values",
        "json_number_reading": "literal_digits",
        "sampled_score_estimator": "population_weighted",
        "date_statistics": "every_value",
        "outlier_spread": "nonzero_iqr",
        "date_forms": "spaced_iso_datetimes",
        "value_facts": "every_value_consistency_precision_accuracy_rules_timeliness",
        "column_fingerprint": "xxh3_128_sum_v1",
    }
    assert report.metric_semantics == expected
    assert report.to_dict()["metric_semantics"] == expected
    assert json.loads(report.to_json())["metric_semantics"] == expected


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
def test_table_sized_distinct_count_is_exact(engine, tmp_path):
    # Past the old 10,000 estimator bound, which made these counts an HLL
    # estimate on every engine (matches the Rust parity test). The exact
    # regime now reaches a million distinct values; the sketch past it is
    # covered by the estimator's own tests.
    n_rows = 20_000
    path = tmp_path / "high_card.csv"
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["id"])
        for value in range(n_rows):
            writer.writerow([value])

    report = dataprof.profile(str(path), engine=engine)
    col = report["id"]
    assert col.unique_count_is_approximate is False, engine
    assert col.unique_count == n_rows, engine


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
        assert uniqueness["key_uniqueness"] is None, f"{engine} has no declared key"

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
