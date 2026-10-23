"""Profile the oracle matrix through every input route and check it against
answers computed without dataprof (#829).

Parity tests compare routes with each other, so they pass when every route is
wrong the same way. `tests/fixtures/oracle-v2.json` holds cases whose expected
values `_oracle_fixture.py` computes with exact rational arithmetic, never
importing dataprof: numeric edges (integers past 2^53, the uint64 boundary,
the 64-value SIMD threshold, cancellation, a variance whose intermediate sum
of squares overflows), Float32 values near a rounding midpoint, decimals at
positive, zero and negative scales, nested values, and the null vocabulary.
Each case runs through every route that can carry it, including Arrow tables
split into uneven and empty batches and Parquet files in each writer layout,
and every disagreement is collected before asserting, so one failing run shows
which routes and fields are wrong.

The fixture holds exact answers, and the comparison rounds them as the output
contract serializes: 4 decimal places, 2 for percentages and quartiles, ties
away from zero. Only a tie the binary64 computation could land on either side
of accepts both neighbours.

`KNOWN` lists the disagreements an open issue owns. It is strict both ways: a
new disagreement fails, and so does a listed one that stops disagreeing, so the
table cannot outlive the fix that empties it.

`tests/oracle.rs` is the Rust twin for the file engines.
"""

from __future__ import annotations

import ast
import csv
import io
import itertools
import json
import math
import sys
from collections.abc import Callable, Iterator
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import Any

import _oracle_fixture
import dataprof as dp
import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
pl = pytest.importorskip("polars")

FIXTURE = _oracle_fixture.FIXTURE
CASES = json.loads(FIXTURE.read_text(encoding="utf-8"))["cases"]

#: The float error a binary64 computation may carry, relative to the exact
#: answer: a tie at the serialized precision closer than this may round either
#: way. The same bound compares the statistics of a `relative` case.
REL_TOLERANCE = 1e-12
ABS_TOLERANCE = 1e-12

#: Decimal places the output contract serializes, by field; 4 otherwise.
PLACES = {"null_percentage": 2, "coefficient_of_variation": 2, "quartiles": 2}

#: (case, route, field) -> the issue that owns the disagreement. A route of
#: "*" covers every route the case runs on.
KNOWN: dict[tuple[str, str, str], str] = {
    # 64-bit integer extrema and order statistics are taken in f64, so values
    # past 2^53 collapse: the extremes round, distinct values tie for a mode,
    # and the quartiles of a narrow spread coincide.
    **{
        (case, "*", field): "#1038"
        for case in ("above_2_pow_53", "epoch_nanoseconds", "uint64_edge")
        for field in ("min", "max", "mode")
    },
    ("epoch_nanoseconds", "*", "quartiles.iqr"): "#1038",
    ("uint64_edge", "*", "quartiles.iqr"): "#1038",
}

Route = tuple[str, Callable[[], dp.ProfileReport]]
Mismatch = tuple[str, str, str, Any, Any]


def _values(case: dict[str, Any]) -> list[Any]:
    if case["kind"] != "numeric":
        return case["values"]
    dtype = case["dtype"]
    if dtype.startswith("decimal"):
        cast: Callable[[str], Any] = Decimal
    elif dtype.startswith("float"):
        # A float32 literal rounds to the nearest binary32 when the typed
        # column is built, as the fixture's answers assume.
        cast = float
    else:
        cast = int
    return [None if value is None else cast(value) for value in case["values"]]


def _arrow_type(case: dict[str, Any]) -> Any:
    dtype = case["dtype"]
    if dtype.startswith("decimal"):
        return getattr(pa, dtype)(case["precision"], case["scale"])
    return getattr(pa, dtype)()


def _batches(table: Any) -> list[Any]:
    """The table as uneven record batches, empty ones among them.

    A leading empty batch, a batch of one row, and the rest of the rows after
    the first batch, so a route that reads only the first batch, or takes its
    schema or statistics from it, disagrees.
    """
    column = table.column(0).combine_chunks()
    sizes = itertools.cycle([0, 1, 0, 2, 3])
    batches, start = [], 0
    while start < len(column):
        size = next(sizes)
        batches.append(pa.RecordBatch.from_arrays([column.slice(start, size)], schema=table.schema))
        start += size
    batches.append(pa.RecordBatch.from_arrays([column.slice(0, 0)], schema=table.schema))
    return batches


def _layouts(column_type: Any) -> dict[str, dict[str, Any]]:
    """Parquet writer settings that store the same column differently (#647)."""
    layouts: dict[str, dict[str, Any]] = {
        # pyarrow's default: dictionary encoding, data page v1, snappy.
        "": {},
        "plain": {"use_dictionary": False},
        "page_v2": {"data_page_version": "2.0"},
        "page_v2_plain": {"data_page_version": "2.0", "use_dictionary": False},
        # Many row groups of several pages each, zstd-compressed.
        "small_pages": {
            "row_group_size": 3,
            "data_page_size": 1,
            "write_batch_size": 2,
            "compression": "zstd",
        },
    }
    encodings = []
    numeric = pa.types.is_floating(column_type) or pa.types.is_integer(column_type)
    if numeric or pa.types.is_decimal(column_type):
        encodings.append("BYTE_STREAM_SPLIT")
    if pa.types.is_integer(column_type):
        encodings.append("DELTA_BINARY_PACKED")
    if pa.types.is_decimal(column_type):
        encodings.append("DELTA_BYTE_ARRAY")
        if column_type.precision <= 18:
            layouts["decimal_as_integer"] = {"store_decimal_as_integer": True}
    if pa.types.is_string(column_type):
        encodings.extend(["DELTA_LENGTH_BYTE_ARRAY", "DELTA_BYTE_ARRAY"])
    for encoding in encodings:
        layouts[encoding.lower()] = {"use_dictionary": False, "column_encoding": {"v": encoding}}
    return layouts


def _routes(case: dict[str, Any], directory: Path) -> Iterator[Route]:
    """Every public route that carries the case's logical data."""
    values = _values(case)
    records = [{"v": value} for value in values]
    wanted = set(case.get("routes", []))

    def allowed(route: str) -> bool:
        return not wanted or route in wanted

    if allowed("dict"):
        yield "dict", lambda: dp.profile({"v": values})
    if allowed("records"):
        yield "records", lambda: dp.profile(records)
    # JSON has no decimal type: a JSON reader holds the binary64 nearest the
    # literal, which is what float() gives.
    plain = [{"v": float(v) if isinstance(v, Decimal) else v} for v in values]
    document = json.dumps(plain, allow_nan=False)
    if allowed("json"):
        path = directory / f"{case['id']}.json"
        path.write_text(document, encoding="utf-8")
        yield "json", lambda path=path: dp.profile(str(path))
        yield "json_bytes", lambda: dp.profile(document.encode(), format="json")
    if allowed("jsonl"):
        path = directory / f"{case['id']}.jsonl"
        path.write_text("".join(json.dumps(row) + "\n" for row in plain), encoding="utf-8")
        yield "jsonl", lambda path=path: dp.profile(str(path))
    if allowed("csv"):
        buffer = io.StringIO(newline="")
        writer = csv.writer(buffer, quoting=csv.QUOTE_ALL)
        writer.writerow(["v"])
        writer.writerows([[value] for value in values])
        text = buffer.getvalue()
        path = directory / f"{case['id']}.csv"
        path.write_text(text, encoding="utf-8", newline="")
        for engine in ("incremental", "columnar"):
            yield (
                f"csv_{engine}",
                lambda engine=engine, path=path: dp.profile(str(path), engine=engine),
            )
        yield "csv_bytes", lambda: dp.profile(text.encode(), format="csv")

    if case["kind"] == "numeric":
        table = pa.table({"v": pa.array(values, type=_arrow_type(case))})
        frame = table.to_pandas()
    else:
        typed = [r for r in _oracle_fixture.TYPED_ROUTES if r != "pandas"]
        table = pa.table({"v": values}) if any(allowed(r) for r in typed) else None
        frame = pd.DataFrame({"v": pd.Series(values, dtype=object)})

    if allowed("pandas"):
        yield "pandas", lambda: dp.profile(frame)
    if table is None:
        return
    if allowed("pandas_arrow"):
        arrow_frame = table.to_pandas(types_mapper=pd.ArrowDtype)
        yield "pandas_arrow", lambda: dp.profile(arrow_frame)
    if allowed("polars"):
        polars_frame = pl.from_arrow(table)
        yield "polars", lambda: dp.profile(polars_frame)
    if allowed("arrow"):
        yield "arrow", lambda: dp.profile(table)
    batches = _batches(table)
    if allowed("arrow_chunked"):
        chunked = pa.Table.from_batches(batches, schema=table.schema)
        yield "arrow_chunked", lambda: dp.profile(chunked)
    if allowed("arrow_stream"):
        yield (
            "arrow_stream",
            lambda: dp.profile(pa.RecordBatchReader.from_batches(table.schema, batches)),
        )
    if allowed("parquet"):
        for layout, options in _layouts(table.schema.field("v").type).items():
            name = f"parquet[{layout}]" if layout else "parquet"
            path = directory / f"{case['id']}-{layout or 'default'}.parquet"
            pq.write_table(table, path, **options)
            yield name, lambda path=path: dp.profile(str(path))


def _half_away(value: Fraction) -> int:
    whole = math.floor(abs(value) + Fraction(1, 2))
    return whole if value >= 0 else -whole


def _serialized(actual: Any, wanted: str, places: int) -> bool:
    """Whether `actual` is the exact `wanted` as the contract serializes it.

    Rounded at `places`, ties away from zero. A binary64 computation of a value
    within float error of a tie may land on either side of it, so there, and
    only there, both neighbours are right.
    """
    if not isinstance(actual, (int, float)) or isinstance(actual, bool):
        return False
    exact = Fraction(wanted)
    slack = abs(exact) * Fraction(REL_TOLERANCE) + Fraction(ABS_TOLERANCE)
    scale = 10**places
    lowest = _half_away((exact - slack) * scale)
    highest = _half_away((exact + slack) * scale)
    scaled = Fraction(actual) * scale
    units = round(scaled)
    # A serialized value sits on the grid, up to its binary representation.
    on_grid = abs(scaled - units) <= Fraction(1, 10**9) * max(1, abs(units))
    return on_grid and lowest <= units <= highest


def _faithful(actual: Any, wanted: str) -> bool:
    """Whether `actual` is a faithful binary64 rendering of the exact `wanted`.

    Past 2^53 adjacent floats are hundreds apart, so the serialized precision
    says nothing there: the bound is half an ulp of the exact value, the error
    rounding to the nearest float may carry.
    """
    if actual is None:
        return False
    exact = Fraction(wanted)
    return abs(Fraction(actual) - exact) <= Fraction(math.ulp(float(exact))) / 2


def _relative(actual: Any, wanted: str) -> bool:
    if actual is None:
        return False
    target = float(Fraction(wanted))
    return math.isclose(float(actual), target, rel_tol=REL_TOLERANCE, abs_tol=0.0)


def _check(case: dict[str, Any], route: str, report: dp.ProfileReport) -> list[Mismatch]:
    """Every field of the case's expectation that `report` gets wrong."""
    document = report.to_dict()
    column = document["columns"][0]
    quality = document.get("quality") or {}
    expected = case["expected"]
    found: list[Mismatch] = []

    def compare(field: str, actual: Any, wanted: Any, how: str = "serialized") -> None:
        if wanted is None:
            good = actual is None
        elif not isinstance(wanted, str) or field == "data_type":
            good = actual == wanted
        elif how == "faithful":
            good = _faithful(actual, wanted)
        elif how == "relative":
            good = _relative(actual, wanted)
        else:
            good = _serialized(actual, wanted, PLACES.get(field.split(".")[0], 4))
        if not good:
            found.append((case["id"], route, field, wanted, actual))

    for field in ("total_count", "null_count", "null_percentage", "unique_count", "data_type"):
        if field in expected:
            compare(field, column.get(field), expected[field])
    if "stats" in expected:
        compare("stats", column.get("stats"), expected["stats"])
    if "duplicate_rows" in expected:
        uniqueness = quality.get("uniqueness") or {}
        compare("duplicate_rows", uniqueness.get("duplicate_rows"), expected["duplicate_rows"])
    if case["kind"] != "numeric":
        return found

    stats = column.get("stats") or {}
    present = [v for v in case["values"] if v is not None]
    # Past 2^53 an exact integer can fall between two binary64 values; either
    # neighbour is a faithful float, so the bound is half an ulp.
    wide = case["dtype"] in ("int64", "uint64") and any(abs(int(v)) > 2**53 for v in present)
    if case.get("relative"):
        # 4dp carries no information at this magnitude.
        for name in ("mean", "variance", "std_dev", "min", "max"):
            compare(name, stats.get(name), expected[name], "relative")
        return found

    for name in ("variance", "std_dev", "skewness", "kurtosis", "coefficient_of_variation"):
        compare(name, stats.get(name), expected[name])
    compare("outlier_count", stats.get("outlier_count"), expected["outlier_count"])
    for name in ("min", "max"):
        if wide:
            # An exact integer extreme needs an integer-preserving value;
            # comparing after float() would erase the defect (#1038).
            actual = stats.get(name)
            if actual is None or Fraction(actual) != Fraction(expected[name]):
                found.append((case["id"], route, name, expected[name], actual))
        else:
            compare(name, stats.get(name), expected[name])
    how = "faithful" if wide else "serialized"
    for name in ("mean", "median", "mode"):
        compare(name, stats.get(name), expected[name], how)
    quartiles = stats.get("quartiles")
    if expected["quartiles"] is None:
        compare("quartiles", quartiles, None)
    else:
        for name, value in expected["quartiles"].items():
            compare(f"quartiles.{name}", (quartiles or {}).get(name), value, how)
    return found


def _owner(mismatch: Mismatch) -> str | None:
    case, route, field = mismatch[:3]
    return KNOWN.get((case, route, field)) or KNOWN.get((case, "*", field))


def test_the_fixture_is_what_the_generator_writes():
    """An expected value changes only by changing its computation."""
    assert FIXTURE.read_text(encoding="utf-8") == _oracle_fixture.render(), (
        "tests/fixtures/oracle-v2.json is stale: run "
        "`uv run python python/tests/_oracle_fixture.py`"
    )


def test_the_generator_imports_only_the_standard_library():
    """An answer computed with dataprof, or a library it shares, is not independent."""
    tree = ast.parse(Path(_oracle_fixture.__file__).read_text(encoding="utf-8"))
    imported = {
        name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for name in (
            [alias.name for alias in node.names]
            if isinstance(node, ast.Import)
            else [node.module or ""]
        )
    }
    assert imported <= sys.stdlib_module_names | {"__future__"}, imported


def test_every_case_names_known_routes():
    known = set(_oracle_fixture.TEXT_ROUTES + _oracle_fixture.TYPED_ROUTES)
    for case in CASES:
        assert set(case.get("routes", [])) <= known, case["id"]


@pytest.mark.parametrize("case", CASES, ids=[case["id"] for case in CASES])
def test_every_route_matches_the_independent_answer(case: dict[str, Any], tmp_path: Path):
    seen: list[tuple[str, str]] = []
    mismatches: list[Mismatch] = []
    for route, run in _routes(case, tmp_path):
        seen.append((case["id"], route))
        mismatches.extend(_check(case, route, run()))

    unexplained = [m for m in mismatches if _owner(m) is None]
    explained = {(m[0], m[1], m[2]) for m in mismatches if _owner(m) is not None}
    # A "*" entry claims every route, so each route it covers must still
    # disagree: a fix on one route narrows the entry instead of hiding there.
    stale = [
        (case["id"], route, key[2])
        for key in KNOWN
        if key[0] == case["id"]
        for _, route in seen
        if key[1] in ("*", route) and (case["id"], route, key[2]) not in explained
    ]
    report = "\n".join(
        f"  {route}: {field} expected {wanted!r}, got {actual!r}"
        for _, route, field, wanted, actual in unexplained
    )
    assert not unexplained, f"{case['id']} ({case.get('dtype', case['kind'])}):\n{report}"
    assert not stale, f"{case['id']}: known disagreements that no longer occur: {stale}"
