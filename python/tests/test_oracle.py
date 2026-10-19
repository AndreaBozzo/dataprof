"""Profile the oracle-v1 matrix through every input route and check it against
answers computed without dataprof (#829).

Parity tests compare routes with each other, so they pass when every route is
wrong the same way. `tests/fixtures/oracle-v1.json` holds 20 cases whose
expected values were computed with exact rational arithmetic and 100-digit
decimals, independently of this code: numeric edges (integers past 2^53, the
uint64 boundary, the 64-value SIMD threshold, cancellation, a variance whose
intermediate sum of squares overflows), nested values, and the null
vocabulary. Each case runs through every route that can carry it, and every
disagreement is collected before asserting, so one failing run shows which
routes and fields are wrong.

`KNOWN` lists the disagreements an open issue owns. It is strict both ways: a
new disagreement fails, and so does a listed one that stops disagreeing, so the
table cannot outlive the fix that empties it.

`tests/oracle.rs` is the Rust twin for the file engines.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
from collections.abc import Callable, Iterator
from fractions import Fraction
from pathlib import Path
from typing import Any

import dataprof as dp
import pytest

pd = pytest.importorskip("pandas")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
pl = pytest.importorskip("polars")

FIXTURE = Path(__file__).parents[2] / "tests/fixtures/oracle-v1.json"
#: The hash published with the fixture on #829, so an edit to an expected
#: value cannot pass unnoticed.
FIXTURE_SHA256 = "fe75498f65bc0b7375d7c0cb09e8c98bbf8999662132f5aff54868a95a495409"
CASES = json.loads(FIXTURE.read_text(encoding="utf-8"))["cases"]

#: Half a unit of the 4dp serialized precision, plus room for the binary
#: representation of the rounded value.
ABS_TOLERANCE = 0.000051
#: For magnitudes where 4dp carries no information (around 1e307).
REL_TOLERANCE = 1e-12

#: (case, route, field) -> the issue that owns the disagreement. A route of
#: "*" covers every route the case runs on.
KNOWN: dict[tuple[str, str, str], str] = {
    # 64-bit integer extrema and order statistics are taken in f64, so values
    # past 2^53 collapse: the extremes round, distinct values tie for a mode,
    # and the quartiles of a narrow spread coincide.
    **{
        (case, "*", field): "#877"
        for case in ("above_2_pow_53", "epoch_nanoseconds", "uint64_edge")
        for field in ("min", "max", "mode")
    },
    ("epoch_nanoseconds", "*", "quartiles.iqr"): "#877",
    ("uint64_edge", "*", "quartiles.iqr"): "#877",
}

Route = tuple[str, Callable[[], dp.ProfileReport]]
Mismatch = tuple[str, str, str, Any, Any]


def _values(case: dict[str, Any]) -> list[Any]:
    if case["kind"] != "numeric":
        return case["values"]
    cast = float if case["dtype"] == "float64" else int
    return [cast(value) for value in case["values"]]


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
    document = json.dumps(records, allow_nan=False)
    if allowed("json"):
        path = directory / f"{case['id']}.json"
        path.write_text(document, encoding="utf-8")
        yield "json", lambda path=path: dp.profile(str(path))
        yield "json_bytes", lambda: dp.profile(document.encode(), format="json")
    if allowed("jsonl"):
        path = directory / f"{case['id']}.jsonl"
        path.write_text("".join(json.dumps(row) + "\n" for row in records), encoding="utf-8")
        yield "jsonl", lambda path=path: dp.profile(str(path))

    # A container in a CSV cell is text, not the same logical value.
    if case["kind"] != "nested":
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
        dtype = case["dtype"]
        frame = pd.DataFrame({"v": pd.Series(values, dtype=dtype)})
        table = pa.table({"v": pa.array(values, type=getattr(pa, dtype)())})
        polars_dtype = {"int64": pl.Int64, "uint64": pl.UInt64, "float64": pl.Float64}[dtype]
        polars_frame = pl.DataFrame({"v": pl.Series(values, dtype=polars_dtype)})
    else:
        frame = pd.DataFrame({"v": pd.Series(values, dtype=object)})
        table = pa.table({"v": values}) if allowed("arrow") else None
        polars_frame = pl.from_arrow(table) if table is not None else None

    if allowed("pandas"):
        yield "pandas", lambda: dp.profile(frame)
    if table is not None:
        if allowed("polars"):
            yield "polars", lambda: dp.profile(polars_frame)
        if allowed("arrow"):
            yield "arrow", lambda: dp.profile(table)
        if allowed("parquet"):
            path = directory / f"{case['id']}.parquet"
            pq.write_table(table, path)
            yield "parquet", lambda path=path: dp.profile(str(path))


def _faithful(actual: Any, wanted: str) -> bool:
    """Whether `actual` is a faithful binary64 rendering of the exact `wanted`.

    Past 2^53 adjacent floats are hundreds apart, so neither a 4dp nor a
    relative tolerance says anything there: the bound is half an ulp of the
    exact value, the error rounding to the nearest float may carry.
    """
    if actual is None:
        return False
    exact = Fraction(wanted)
    return abs(Fraction(actual) - exact) <= Fraction(math.ulp(float(exact))) / 2


def _close(actual: Any, wanted: str, relative: bool) -> bool:
    if actual is None:
        return False
    target = float(Fraction(wanted))
    if relative:
        return math.isclose(float(actual), target, rel_tol=REL_TOLERANCE, abs_tol=0.0)
    return math.isclose(float(actual), target, rel_tol=0.0, abs_tol=ABS_TOLERANCE)


def _check(case: dict[str, Any], route: str, report: dp.ProfileReport) -> list[Mismatch]:
    """Every field of the case's expectation that `report` gets wrong."""
    column = report.to_dict()["columns"][0]
    quality = report.to_dict().get("quality") or {}
    expected = case.get("expected") or case["expected_common_markers"]
    found: list[Mismatch] = []

    def compare(
        field: str, actual: Any, wanted: Any, relative: bool = False, faithful: bool = False
    ) -> None:
        if wanted is None:
            good = actual is None
        elif isinstance(wanted, str) and field != "data_type":
            good = _faithful(actual, wanted) if faithful else _close(actual, wanted, relative)
        else:
            good = actual == wanted
        if not good:
            found.append((case["id"], route, field, wanted, actual))

    for field in ("total_count", "null_count", "unique_count", "data_type", "stats"):
        if field in expected:
            compare(field, column.get(field), expected[field])
    if "null_percentage" in expected:
        compare("null_percentage", column.get("null_percentage"), expected["null_percentage"])
    if "duplicate_rows_single_column" in expected:
        uniqueness = quality.get("uniqueness") or {}
        compare(
            "duplicate_rows",
            uniqueness.get("duplicate_rows"),
            expected["duplicate_rows_single_column"],
        )
    if case["kind"] != "numeric":
        return found

    stats = column.get("stats") or {}
    # Past 2^53 an exact integer mean can fall between two binary64 values;
    # either neighbour is a faithful float, so the bound is half an ulp.
    wide = case["dtype"] != "float64" and any(abs(int(v)) > 2**53 for v in case["values"])
    # Around 1e307 the 4dp rounding carries no information; compare relatively.
    extreme = case["id"] == "variance_fits_m2_does_not"
    variance = expected["sample_variance_binary64"] if extreme else expected["variance_4dp"]
    compare("variance", stats.get("variance"), variance, relative=extreme)
    compare("std_dev", stats.get("std_dev"), expected["std_dev_4dp"], relative=extreme)
    if wide:
        compare("mean", stats.get("mean"), expected["mean_exact"], faithful=True)
    else:
        compare("mean", stats.get("mean"), expected["mean_4dp"], relative=extreme)
    for name in ("min", "max"):
        exact = Fraction(expected[f"{name}_exact"])
        actual = stats.get(name)
        if wide:
            # An exact integer extreme needs an integer-preserving value;
            # comparing after float() would erase the defect (#877).
            if actual is None or Fraction(actual) != exact:
                found.append((case["id"], route, name, str(exact), actual))
        else:
            compare(name, actual, str(float(exact)), relative=extreme)
    if extreme:
        return found
    compare("skewness", stats.get("skewness"), expected["adjusted_sample_skewness_4dp"])
    compare("kurtosis", stats.get("kurtosis"), expected["adjusted_sample_excess_kurtosis_4dp"])
    compare("median", stats.get("median"), expected["median_exact"], faithful=wide)
    compare("mode", stats.get("mode"), expected["mode_exact"], faithful=wide)
    compare("outlier_count", stats.get("outlier_count"), expected["outlier_count"])
    quartiles = stats.get("quartiles")
    if expected["quartiles_4dp"] is None:
        compare("quartiles", quartiles, None)
    else:
        for name, value in expected["quartiles_4dp"].items():
            compare(f"quartiles.{name}", (quartiles or {}).get(name), value, faithful=wide)
    return found


def _owner(mismatch: Mismatch) -> str | None:
    case, route, field = mismatch[:3]
    return KNOWN.get((case, route, field)) or KNOWN.get((case, "*", field))


def test_the_fixture_is_the_published_one():
    assert hashlib.sha256(FIXTURE.read_bytes()).hexdigest() == FIXTURE_SHA256


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
    assert not unexplained, f"{case['id']} disagrees with the oracle:\n{report}"
    assert not stale, f"{case['id']}: known disagreements that no longer occur: {stale}"
