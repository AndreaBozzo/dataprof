"""Every Arrow decimal width profiles the same values the same way (#1157).

The batch analyzer read ``decimal128`` and ``decimal256`` as decimals and had no
arm for ``decimal32`` or ``decimal64``, which went through the generic text
formatter. A scale-0 column then typed as an integer where the same values as a
``decimal128`` are a float, and 17 digits of cents read as the float they might
be, so a ``decimal64(18, 2)`` column reported a variance three times the exact
one. Each column is built once per width that holds it, and every width has to
serialize what ``decimal128`` does on the same route. A column longer than the
sample the analyzer keeps checks that each value reaches the exact accumulators,
which the text formatter never fed. ``tests/decimal_widths.rs`` is the Rust twin.
"""

from __future__ import annotations

import io
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

try:
    import dataprof as dp
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
if not hasattr(pa, "decimal32"):
    # pyarrow 19 added both widths; the declared floor is older.
    pytest.skip("pyarrow without decimal32 and decimal64", allow_module_level=True)

WIDTHS = (32, 64, 128, 256)
MAX_PRECISION = {32: 9, 64: 18, 128: 38, 256: 76}
#: name -> (precision, scale, values): a repeated value in each, one negative.
COLUMNS: dict[str, tuple[int, int, list[str]]] = {
    "whole": (9, 0, ["1", "22", "-3", "123456789", "22"]),
    "cents": (9, 2, ["1234567.01", "1234567.05", "-1234567.02", "1234567.09", "1234567.05"]),
    # 18 digits: past a decimal32, and past what an f64 keeps apart.
    "wide_cents": (
        18,
        2,
        [
            "999999999999999.01",
            "999999999999999.05",
            "999999999999999.02",
            "999999999999999.09",
            "999999999999999.05",
        ],
    ),
}
#: Parquet stores no negative scale, so this one is checked in memory only.
NEGATIVE_SCALE = {"hundreds": (5, -2, ["100", "300", "1200", "-500", "300"])}
#: Past the 10,000 numeric values the analyzer samples: cents from -99.99 to
#: 99.99, and the maximum, 250.00, at the first row.
LONG = {
    "long_cents": (
        9,
        2,
        ["250.00", *(str(Decimal(row - 10_000) / 100) for row in range(1, 20_000))],
    )
}


def _table(width: int, columns: dict[str, tuple[int, int, list[str]]]) -> Any:
    make = getattr(pa, f"decimal{width}")
    return pa.table(
        {
            name: pa.array([Decimal(v) for v in values], make(precision, scale))
            for name, (precision, scale, values) in columns.items()
            if precision <= MAX_PRECISION[width]
        }
    )


def _stream(table: Any) -> Any:
    batches = table.to_batches(max_chunksize=2)
    assert len(batches) > 1
    return pa.RecordBatchReader.from_batches(table.schema, batches)


def _parquet_bytes(table: Any) -> bytes:
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue()


def _columns(report: dp.ProfileReport) -> dict[str, dict[str, Any]]:
    return {column["name"]: column for column in report.to_dict()["columns"]}


def _wrong_against_decimal128(
    route: Callable[[Any], dp.ProfileReport],
    columns: dict[str, tuple[int, int, list[str]]],
) -> dict[str, Any]:
    reference = _columns(route(_table(128, columns)))
    assert set(reference) == set(columns)
    wrong = {}
    for width in WIDTHS:
        for name, profiled in _columns(route(_table(width, columns))).items():
            expected = reference[name]
            if profiled != expected:
                wrong[f"decimal{width} {name}"] = (
                    (profiled["data_type"], (profiled["stats"] or {}).get("variance")),
                    (expected["data_type"], (expected["stats"] or {}).get("variance")),
                )
    return wrong


ROUTES: dict[str, Callable[[Any], dp.ProfileReport]] = {
    "arrow table": dp.profile,
    "arrow stream": lambda table: dp.profile(_stream(table)),
    "parquet bytes": lambda table: dp.profile(_parquet_bytes(table), format="parquet"),
}


@pytest.mark.parametrize("route", ROUTES)
def test_every_width_profiles_like_decimal128(route: str):
    wrong = _wrong_against_decimal128(ROUTES[route], COLUMNS)
    assert not wrong, (route, wrong)


def test_parquet_file_and_schema(tmp_path: Path):
    def from_file(table: Any) -> dp.ProfileReport:
        path = tmp_path / "widths.parquet"
        pq.write_table(table, path)
        return dp.profile(str(path))

    wrong = _wrong_against_decimal128(from_file, COLUMNS)
    for width in WIDTHS:
        path = tmp_path / f"decimal{width}.parquet"
        pq.write_table(_table(width, COLUMNS), path)
        for column in dp.infer_schema(str(path)).columns:
            if column["data_type"] != "float":
                wrong[f"decimal{width} infer_schema {column['name']}"] = column["data_type"]
    assert not wrong, wrong


@pytest.mark.parametrize("route", ROUTES)
def test_a_column_past_the_sample_is_read_whole(route: str):
    stats = _columns(ROUTES[route](_table(128, LONG)))["long_cents"]["stats"]
    # Every value but the first cancels out: only the whole column has this mean.
    assert (stats["min"], stats["max"], stats["mean"]) == (-99.99, 250.0, 0.0125)
    wrong = _wrong_against_decimal128(ROUTES[route], LONG)
    assert not wrong, (route, wrong)


@pytest.mark.parametrize("route", ["arrow table", "arrow stream"])
def test_a_negative_scale_in_every_width(route: str):
    wrong = _wrong_against_decimal128(ROUTES[route], NEGATIVE_SCALE)
    assert not wrong, (route, wrong)
