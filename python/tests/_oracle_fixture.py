"""Write `tests/fixtures/oracle-v2.json`, the independent answers that
`test_oracle.py` and `tests/oracle.rs` check every input route against (#829).

Standard library only, and never dataprof: the answers come from exact
rational arithmetic (`fractions.Fraction`) and 100-digit `decimal.Decimal`,
so they cannot share a defect with the code under test. `test_oracle.py`
rebuilds the fixture from this module and fails if the two differ, so an
expected value changes only by changing the computation here.

Run after editing: `uv run python python/tests/_oracle_fixture.py`

Every answer is exact. Rational ones are strings such as `"5/3"`; irrational
ones (standard deviation, skewness, kurtosis) are decimals to 40 significant
digits. The tests round them to the precision the output contract serializes,
so the fixture states the truth and the contract stays in the tests. Inputs
are strings too, read according to `dtype`, so no JSON tool can round them
before the profiler sees them.
"""

from __future__ import annotations

import json
import struct
from collections import Counter
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from fractions import Fraction
from pathlib import Path
from typing import Any

FIXTURE = Path(__file__).parents[2] / "tests/fixtures/oracle-v2.json"

#: Every route the tests know, by the name a case's `routes` list uses. A
#: case without a list runs on all of them. `csv` covers both engines and CSV
#: bytes, `json` covers JSON files and bytes, `parquet` every writer layout.
TEXT_ROUTES = ["dict", "records", "json", "jsonl", "csv"]
TYPED_ROUTES = [
    "pandas",
    "pandas_arrow",
    "polars",
    "arrow",
    "arrow_chunked",
    "arrow_stream",
    "parquet",
]


def decimal_of(value: Fraction) -> Decimal:
    with localcontext() as ctx:
        ctx.prec = 100
        return Decimal(value.numerator) / Decimal(value.denominator)


def digits(value: Decimal | None) -> str | None:
    """An irrational answer to 40 significant digits, in plain notation."""
    if value is None:
        return None
    if value == 0:
        return "0"
    with localcontext() as ctx:
        ctx.prec = 100
        quantum = Decimal(1).scaleb(value.adjusted() - 39)
        return format(value.quantize(quantum, rounding=ROUND_HALF_EVEN), "f")


def exact(text: str, dtype: str) -> Fraction:
    """The value a typed input holds for the literal `text`."""
    if dtype in ("int64", "uint64"):
        return Fraction(int(text))
    if dtype == "float64":
        # The exact rational value of the supplied IEEE binary64.
        return Fraction(float(text))
    if dtype == "float32":
        # The literal rounds to the nearest binary32; its exact value is what
        # a lossless promotion to binary64 holds.
        (single,) = struct.unpack("<f", struct.pack("<f", float(text)))
        return Fraction(single)
    if dtype.startswith("decimal"):
        return Fraction(Decimal(text))
    raise ValueError(f"unknown dtype {dtype}")


def type7(values: list[Fraction], p: Fraction) -> Fraction:
    values = sorted(values)
    position = (len(values) - 1) * p
    lower = position.numerator // position.denominator
    upper = min(lower + 1, len(values) - 1)
    weight = position - lower
    return values[lower] * (1 - weight) + values[upper] * weight


def text(value: Fraction | None) -> str | None:
    return None if value is None else str(value)


def numeric_case(
    case_id: str,
    values: list[str | None],
    dtype: str = "int64",
    *,
    precision: int | None = None,
    scale: int | None = None,
    routes: list[str] | None = None,
    relative: bool = False,
    notes: list[str] | None = None,
) -> dict[str, Any]:
    """One numeric column. `None` is a null; the statistics are over the rest."""
    present = [exact(v, dtype) for v in values if v is not None]
    if scale is not None:
        # A literal the declared scale cannot hold would be rounded by the
        # writer, and the case would no longer describe its own input.
        for value in present:
            assert (value * Fraction(10) ** scale).denominator == 1, (case_id, value)
    n = len(present)
    mean = sum(present, Fraction(0)) / n
    m2 = sum(((v - mean) ** 2 for v in present), Fraction(0))
    variance = m2 / (n - 1) if n > 1 else None
    with localcontext() as ctx:
        ctx.prec = 100
        std = decimal_of(variance).sqrt() if variance is not None else None
        skew = kurtosis = None
        if n >= 3 and std is not None:
            skew = (
                Decimal(0)
                if m2 == 0
                else Decimal(n)
                / Decimal((n - 1) * (n - 2))
                * sum(decimal_of(v - mean) ** 3 for v in present)
                / std**3
            )
        if n >= 4 and std is not None:
            kurtosis = (
                Decimal(0)
                if m2 == 0
                else Decimal(n * (n + 1))
                / Decimal((n - 1) * (n - 2) * (n - 3))
                * sum(decimal_of(v - mean) ** 4 for v in present)
                / std**4
                - Decimal(3 * (n - 1) ** 2) / Decimal((n - 2) * (n - 3))
            )
        # Absent when the mean is zero: the ratio is undefined there.
        cv = std / abs(decimal_of(mean)) * 100 if std is not None and mean != 0 else None
    counts = Counter(present)
    top = max(counts.values())
    mode = min(v for v, count in counts.items() if count == top) if top > 1 else None
    quartiles = outliers = None
    if n >= 4:
        q1, q2, q3 = (type7(present, Fraction(k, 4)) for k in (1, 2, 3))
        quartiles = {"q1": text(q1), "q2": text(q2), "q3": text(q3), "iqr": text(q3 - q1)}
        low, high = q1 - Fraction(3, 2) * (q3 - q1), q3 + Fraction(3, 2) * (q3 - q1)
        outliers = sum(v < low or v > high for v in present)
    case: dict[str, Any] = {
        "id": case_id,
        "kind": "numeric",
        "column": "v",
        "dtype": dtype,
    }
    if precision is not None:
        case["precision"] = precision
        case["scale"] = scale
    if routes is not None:
        case["routes"] = routes
    if relative:
        case["relative"] = True
    case["values"] = values
    case["expected"] = {
        "total_count": len(values),
        "null_count": len(values) - n,
        "null_percentage": text(Fraction(100 * (len(values) - n), len(values))),
        "unique_count": len(counts),
        # Row identity: two nulls are the same row.
        "duplicate_rows": len(values) - len(set(counts) | ({None} if n < len(values) else set())),
        "min": text(min(present)),
        "max": text(max(present)),
        "mean": text(mean),
        "variance": text(variance),
        "std_dev": digits(std),
        "coefficient_of_variation": digits(cv),
        "median": text(type7(present, Fraction(1, 2))),
        "mode": text(mode),
        "quartiles": quartiles,
        "outlier_count": outliers,
        "skewness": digits(skew),
        "kurtosis": digits(kurtosis),
    }
    case["notes"] = notes or []
    return case


def canonical(value: Any) -> tuple[str, ...]:
    # Tag containers separately from scalar text. Dict order is immaterial;
    # list order is material. These cases contain only RFC 8259 JSON values.
    if isinstance(value, (list, dict)):
        return ("container", json.dumps(value, sort_keys=True, separators=(",", ":")))
    return ("null",) if value is None else ("scalar", type(value).__name__, str(value))


def nested_case(case_id: str, values: list[Any], homogeneous: bool = True) -> dict[str, Any]:
    # An Arrow stream rejects a nested column with an error, so the stream
    # route cannot carry these.
    typed = ["arrow", "arrow_chunked", "parquet", "polars"]
    return {
        "id": case_id,
        "kind": "nested",
        "column": "v",
        "routes": ["dict", "records", "json", "jsonl", "pandas"] + (typed if homogeneous else []),
        "values": values,
        "expected": {
            "total_count": len(values),
            "null_count": sum(v is None for v in values),
            "duplicate_rows": len(values) - len({canonical(v) for v in values}),
            **({"data_type": "nested", "stats": None, "unique_count": None} if homogeneous else {}),
        },
    }


def build() -> dict[str, Any]:
    cases = [
        numeric_case("ordinary", ["1", "2", "3", "4"]),
        numeric_case("mixed_sign_zero_mean", ["-10", "-2", "0", "4", "8"]),
        numeric_case(
            "constant",
            ["7", "7", "7", "7"],
            notes=[
                "Zero shape moments are dataprof's explicit constant-column convention; "
                "mathematically standardized moments are undefined."
            ],
        ),
        numeric_case("two_values", ["1", "2"]),
        numeric_case("asymmetric", ["1", "2", "3", "10", "4", "5"]),
        numeric_case(
            "above_2_pow_53",
            ["9007199254740993", "9007199254740995", "9007199254740997"],
            notes=["#1038: exact extrema and order statistics past 2^53."],
        ),
        numeric_case(
            "epoch_nanoseconds",
            [
                "1759700000123456789",
                "1759700000123456791",
                "1759700000123456795",
                "1759700000123456800",
            ],
            notes=["#1038: extrema, mode and IQR; variance and shape must hold now."],
        ),
        numeric_case(
            "uint64_edge",
            [
                "18446744073709551612",
                "18446744073709551613",
                "18446744073709551614",
                "18446744073709551615",
            ],
            "uint64",
            notes=["Never build a float64 series first. #1038 applies to order statistics."],
        ),
        numeric_case("simd_64", ["0", "1", "2", "3"] * 16),
        numeric_case("simd_65", ["0", "1", "2", "3"] * 16 + ["0"]),
        numeric_case("large_offset_64", [str(1000000000000 + i) for i in [0, 1, 2, 3] * 16]),
        numeric_case("cancellation", ["-1000000000000.0", "1.0", "1000000000000.0"], "float64"),
        numeric_case(
            "variance_fits_m2_does_not",
            ["-7e+153", "7e+153", "-7e+153", "7e+153"],
            "float64",
            relative=True,
            notes=[
                "Statistics compare relatively and not-null: 4dp says nothing at this magnitude. "
                "Higher moments and outlier fences are outside this case."
            ],
        ),
        # Float32: the value a column holds is the binary32 nearest its
        # literal, and every statistic is of that value promoted losslessly.
        # No text spelling holds the same value on every text route, so these
        # run on the typed ones.
        numeric_case(
            "float32_constant",
            ["1.23495"] * 4,
            "float32",
            routes=TYPED_ROUTES,
            notes=[
                "#1041: 1.23495 is held as 1.2349499464..., so every data value serializes as "
                "1.2349; a median read from the short text 1.235 lies above the max."
            ],
        ),
        numeric_case(
            "float32_below_the_midpoint",
            ["1.00005", "-1.00005", None, "3.14165", "-0.00005", "1.00005", "12.34565", "2.67505"],
            "float32",
            routes=TYPED_ROUTES,
            notes=[
                "Each literal but 2.67505 is a 4dp midpoint in decimal and lies below it in "
                "binary32, on both signs, so rounding the text instead of the held value moves "
                "each data value by one unit."
            ],
        ),
        numeric_case(
            "float32_integer_spacing",
            ["16777216", "16777217", "16777218", "16777219", "16777221"],
            "float32",
            routes=TYPED_ROUTES,
            notes=["Past 2^24 binary32 holds only even integers: 16777217 is 16777216."],
        ),
        # Decimals: a decimal is its unscaled integer times 10^-scale (#821).
        numeric_case(
            "decimal128_scale_2",
            ["1.25", "-3.50", None, "1.25", "10.00", "0.01", "-0.07"],
            "decimal128",
            precision=5,
            scale=2,
        ),
        numeric_case(
            "decimal128_scale_0",
            ["123", "-45", None, "0", "123", "999999999"],
            "decimal128",
            precision=10,
            scale=0,
        ),
        numeric_case(
            "decimal128_scale_10",
            ["1234567.8901234567", "-0.0000000001", None, "1234567.8901234567", "42.5"],
            "decimal128",
            precision=38,
            scale=10,
            notes=["Text routes read these as binary64, which keeps 4dp at this magnitude."],
        ),
        numeric_case(
            "decimal128_negative_scale",
            ["12300", "-500", None, "0", "12300", "700"],
            "decimal128",
            precision=5,
            scale=-2,
            routes=TEXT_ROUTES
            + ["pandas", "pandas_arrow", "arrow", "arrow_chunked", "arrow_stream"],
            notes=[
                "Parquet's decimal type and polars reject a negative scale, so those routes cannot"
                " carry the column."
            ],
        ),
        numeric_case(
            "decimal256_scale_4",
            ["12345.6789", "-0.0001", None, "12345.6789", "-99999.9999", "0.5000"],
            "decimal256",
            precision=40,
            scale=4,
            routes=TEXT_ROUTES
            + ["pandas", "pandas_arrow", "arrow", "arrow_chunked", "arrow_stream", "parquet"],
            notes=["polars has no 256-bit decimal and panics importing one."],
        ),
        nested_case(
            "object_key_order", [{"a": 1, "b": 2}, {"b": 2, "a": 1}, {"a": 2, "b": 2}, None]
        ),
        nested_case("list_order", [[1, 2], [2, 1], [1, 2], [], None]),
        nested_case("inner_null_is_not_outer_null", [{"x": None}, {"x": 1}, None, {"x": None}]),
        nested_case("container_vs_its_text", [{"x": 1}, '{"x":1}', [1, 2], "[1,2]"], False),
        nested_case(
            "container_and_text_repeats",
            [{"x": 1}, '{"x":1}', [1, 2], "[1,2]", {"x": 1}, '{"x":1}'],
            False,
        ),
    ]
    nulls = [None, "", " ", "\t", "null", "NULL", "NaN", "NA", "N/A", "n/a", "#N/A", "\\N", "None"]
    near_misses = ["Na", "na", "none", "NONE", "#n/a", "\\n", "N/a", "-", "?", "n.d.", "0"]
    values = nulls + near_misses
    cases.append(
        {
            "id": "null_tokens_and_near_misses",
            "kind": "nulls",
            "column": "v",
            "values": values,
            "expected": {
                "total_count": len(values),
                "null_count": len(nulls),
                "null_percentage": text(Fraction(100 * len(nulls), len(values))),
                "unique_count": len(near_misses),
                # Row identity is lexical: only None and "" collide.
                "duplicate_rows": 1,
            },
            "expected_typed_only": {"null_count": 1, "null_percentage": text(Fraction(100, 24))},
            "notes": [
                "Duplicate-row identity is raw lexical identity, not equality after the null "
                "vocabulary: only None and the empty string collide here.",
                "Typed-only mode is a proposed #846 setting, not checked yet.",
            ],
        }
    )
    cases.append(
        {
            "id": "all_null",
            "kind": "nulls",
            "column": "v",
            "values": [None] * 4,
            "expected": {
                "total_count": 4,
                "null_count": 4,
                "null_percentage": "100",
                "unique_count": 0,
                "stats": None,
                "duplicate_rows": 3,
            },
            "notes": [],
        }
    )
    return {
        "fixture_version": 2,
        "generator": "python/tests/_oracle_fixture.py",
        "oracle": "fractions.Fraction + 100-digit decimal.Decimal; no dataprof imports",
        "conventions": {
            "variance": "sample, ddof=1",
            "quartiles": "Hyndman-Fan type 7; absent below four observations",
            "mode": "absent when all unique; smallest tied modal value",
            "coefficient_of_variation": "std_dev / |mean| * 100; absent when the mean is zero",
            "undefined": "null, never zero except the documented constant shape convention",
            "values": (
                "strings read by dtype; float64 is the exact binary64, float32 the nearest"
                " binary32 promoted losslessly, decimals exact"
            ),
            "answers": (
                "exact: rationals as p/q, irrationals to 40 significant digits; the tests"
                " round them to the serialized precision"
            ),
        },
        "cases": cases,
    }


def render() -> str:
    return json.dumps(build(), indent=2, ensure_ascii=False, allow_nan=False) + "\n"


if __name__ == "__main__":
    FIXTURE.write_text(render(), encoding="utf-8", newline="\n")
    print(f"wrote {FIXTURE}")
