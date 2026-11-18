"""The floating median and quartiles of a column held exactly (#1334).

`tests/exact_median_interpolation.rs` is the Rust twin, over the text file
routes. Here the routes are the Arrow ones: a pyarrow table, a one-shot Arrow
C stream, Parquet bytes (sync and async), and a dict of Python ints.
"""

from __future__ import annotations

import asyncio
import io
from collections.abc import Callable
from decimal import Decimal
from fractions import Fraction
from typing import Any

import dataprof as dp
import pytest
from dataprof.asyncio import profile_bytes

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

#: (case, Arrow array, median, [q1, q2, q3] or None below four values)
CASES: list[tuple[str, Any, float, list[float] | None]] = [
    (
        "the issue's pair",
        pa.array([-9007199254740992, 9007199254740993], pa.int64()),
        0.5,
        None,
    ),
    (
        "four values: every quartile interpolates past 2^53",
        pa.array(
            [-9007199254740994, -9007199254740992, 9007199254740993, 9007199254740995],
            pa.int64(),
        ),
        0.5,
        [-9007199254740992.0, 0.5, 9007199254740994.0],
    ),
    (
        "decimal128(22, 2)",
        pa.array(
            [Decimal("-1000000000000000000.00"), Decimal("1000000000000000000.02")],
            pa.decimal128(22, 2),
        ),
        0.01,
        None,
    ),
    ("small integers: unchanged", pa.array([1, 2, 3, 4], pa.int64()), 2.5, [1.75, 2.5, 3.25]),
    (
        "decimal128(38, 0): a gap past i128 (#1341)",
        pa.array([Decimal(-(9 * 10**37)), Decimal(9 * 10**37 + 10**20)], pa.decimal128(38, 0)),
        5e19,
        None,
    ),
    (
        "decimal128(38, 0): q2 across a gap past i128 (#1341)",
        pa.array(
            [Decimal(-(9 * 10**37))] * 2 + [Decimal(9 * 10**37 + 10**20)] * 2,
            pa.decimal128(38, 0),
        ),
        5e19,
        [-9e37, 5e19, float(9 * 10**37 + 10**20)],
    ),
]


class _Stream:
    """A one-shot producer that only offers the Arrow C stream."""

    def __init__(self, table: Any) -> None:
        self.reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
        return self.reader.__arrow_c_stream__(requested_schema)


def _parquet(table: Any) -> bytes:
    sink = io.BytesIO()
    pq.write_table(table, sink)
    return sink.getvalue()


ROUTES: dict[str, Callable[[Any], dp.ProfileReport]] = {
    "arrow table": dp.profile,
    "arrow c stream": lambda table: dp.profile(_Stream(table)),
    "parquet bytes": lambda table: dp.profile(_parquet(table), format="parquet"),
    "async parquet bytes": lambda table: asyncio.run(
        profile_bytes(_parquet(table), format="parquet")
    ),
    "dict of python values": lambda table: dp.profile(table.to_pydict()),
}


def test_the_plain_median_and_quartiles_round_the_exact_ones():
    wrong = []
    for case, array, median, quartiles in CASES:
        table = pa.table({"amount": array})
        for route, profile in ROUTES.items():
            if route == "dict of python values" and pa.types.is_decimal(array.type):
                # `Decimal` cells reach the core as text and type a float
                # column, which keeps its `f64` median (see the Rust twin).
                continue
            stats = profile(table).to_dict()["columns"][0]["stats"]
            got = (
                stats["median"],
                [stats["quartiles"][q] for q in ("q1", "q2", "q3")]
                if stats.get("quartiles")
                else None,
            )
            if got != (median, quartiles):
                wrong.append(f"{case} [{route}]: {got}")
            exact = stats.get("exact")
            if exact is not None and float(exact["median"]) != stats["median"]:
                wrong.append(
                    f"{case} [{route}]: median {stats['median']} vs exact {exact['median']}"
                )
    assert not wrong, "\n".join(wrong)


def test_a_short_decimal_column_serializes_what_its_float_twin_does():
    # The exact q1 is 0.145, whose nearest `f64` rounds to 0.14, and q3 0.325
    # rounds to 0.33; `f64` interpolation gives 0.14500000000000002 and
    # 0.32499999999999996, which round to 0.15 and 0.32. Values that fit `f64`
    # stay on it, so the declared decimal serializes what the same values
    # typed `float64` or written as text do.
    values = ["0.01", "0.19", "0.30", "0.40"]
    twins = {
        "decimal128(4, 2)": pa.array([Decimal(v) for v in values], pa.decimal128(4, 2)),
        "float64": pa.array([float(v) for v in values], pa.float64()),
        "text": pa.array(values, pa.string()),
    }
    wrong = []
    for route, profile in ROUTES.items():
        seen = {}
        for kind, array in twins.items():
            stats = profile(pa.table({"amount": array})).to_dict()["columns"][0]["stats"]
            seen[kind] = (stats["median"], stats["quartiles"])
        if len({repr(got) for got in seen.values()}) != 1:
            wrong.append(f"[{route}]: {seen}")
    assert not wrong, "\n".join(wrong)


def test_a_float_column_keeps_the_true_midpoint_of_its_doubles():
    # Rust writes these doubles out in digits (`44918740094933100000`). The
    # outer two have 18 or more of them, so they read as exact integers and the
    # column is held wide; the middle two then read as their shortest digits,
    # which are not the doubles. Interpolating on those would give the median
    # one `f64` step below the true midpoint of the middle two.
    a, b = 4.49187400949331e19, 4.531843763707753e19
    true_midpoint = float((Fraction(a) + Fraction(b)) / 2)
    values = [1.2345678901234566e17, a, b, 9.876543210987654e19]
    table = pa.table({"amount": pa.array(values, pa.float64())})
    wrong = []
    for route, profile in ROUTES.items():
        median = profile(table).to_dict()["columns"][0]["stats"]["median"]
        if median != true_midpoint:
            wrong.append(f"[{route}]: {median!r}, expected {true_midpoint!r}")
    assert not wrong, "\n".join(wrong)
