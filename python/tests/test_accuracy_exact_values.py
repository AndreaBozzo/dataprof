"""The accuracy dimension reads values exactly where the column statistics do
(#1181).

``WIDE`` holds 64-bit integers past 2^53, one about 6 below the lower Tukey
fence: the column's ``outlier_count`` found it, while ``accuracy.outlier_ratio``
reparsed every cell as a float, where values sit 256 apart, and missed it.
``CENTS`` is a decimal column on 10^13 whose lowest value lies 0.00375 below
the lower fence, past what float fences resolve; the typed routes hand it
over as a decimal. The range and sign rules compare each value with its
bounds exactly too: ``100.000000000000000001`` is above 100, ``-1e-400``
below 0, and ``1e400`` past every bound. Only text carries those literals.

``tests/accuracy_exact_values.rs`` is the Rust twin.
"""

from __future__ import annotations

import importlib.util
import json
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

WIDE = [
    1759700000123456512,
    1759700000123456916,
    1759700000123456940,
    1759700000123456945,
    1759700000123457061,
    1759700000123457175,
    1759700000123457320,
    1759700000123457409,
]
CENTS = [
    Decimal("9999999999999") + Decimal(c) / 100 for c in (80, 173, 195, 209, 222, 255, 284, 331)
]

#: Column, values, and the expected range_violations, rule_values_checked and
#: rule_violating_values. Each column also holds a value on each of its bounds.
RULE_CASES: list[tuple[str, list[str], tuple[int, int, int]]] = [
    ("discount_percent", ["100.000000000000000001", "100", "0", "50"], (1, 4, 1)),
    ("customer_age", ["1e400", "150", "0", "30"], (1, 4, 1)),
    ("item_count", ["-1e-400", "0", "5", "1"], (1, 4, 1)),
    # Just inside each bound.
    (
        "birth_year",
        ["1900.000000000000000001", "2099.999999999999999999", "1900", "2100"],
        (0, 4, 0),
    ),
]

Route = Callable[[list[Any]], Any]


def _outliers(report: Any) -> tuple[Any, Any]:
    document = report.to_dict()
    return (
        document["columns"][0]["stats"]["outlier_count"],
        document["quality"]["accuracy"]["outlier_ratio"],
    )


def _wide_routes(tmp_path: Path) -> dict[str, Route]:
    def csv_file(engine: str) -> Route:
        def route(values: list[Any]) -> Any:
            path = tmp_path / f"{engine}.csv"
            path.write_text("\n".join(["v", *map(str, values)]) + "\n", encoding="utf-8")
            return dp.profile(str(path), engine=engine)

        return route

    def json_file(values: list[Any]) -> Any:
        path = tmp_path / "table.json"
        path.write_text(json.dumps([{"v": v} for v in values]), encoding="utf-8")
        return dp.profile(str(path))

    def jsonl_file(values: list[Any]) -> Any:
        path = tmp_path / "table.jsonl"
        path.write_text("\n".join(json.dumps({"v": v}) for v in values) + "\n", encoding="utf-8")
        return dp.profile(str(path))

    def arrow_table(values: list[Any]) -> Any:
        import pyarrow as pa

        return pa.table({"v": pa.array(values, pa.int64())})

    def parquet_file(values: list[Any]) -> Any:
        import pyarrow.parquet as pq

        path = tmp_path / "table.parquet"
        pq.write_table(arrow_table(values), path)
        return dp.profile(str(path))

    routes: dict[str, Route] = {
        "auto": csv_file("auto"),
        "incremental": csv_file("incremental"),
        "columnar": csv_file("columnar"),
        "bytes": lambda values: dp.profile(
            ("v\n" + "\n".join(map(str, values)) + "\n").encode(), format="csv"
        ),
        "json": json_file,
        "jsonl": jsonl_file,
        "dict": lambda values: dp.profile({"v": values}),
        "rows": lambda values: dp.profile([{"v": v} for v in values]),
    }
    if importlib.util.find_spec("pyarrow") is not None:
        routes["arrow"] = lambda values: dp.profile(arrow_table(values))
        routes["parquet"] = parquet_file
    return routes


def test_outlier_ratio_agrees_with_the_column_count_on_wide_integers(tmp_path: Path):
    wrong = []
    for label, route in _wide_routes(tmp_path).items():
        observed = _outliers(route(WIDE))
        if observed != (1, 12.5):
            wrong.append(f"[{label}] {observed}")
    assert not wrong, "\n".join(wrong)


def test_outlier_ratio_reads_a_decimal_column_as_a_decimal(tmp_path: Path):
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    table = pa.table({"v": pa.array(CENTS, pa.decimal128(16, 2))})
    path = tmp_path / "cents.parquet"
    pq.write_table(table, path)
    wrong = []
    for label, source in [("arrow", table), ("parquet", str(path))]:
        observed = _outliers(dp.profile(source))
        if observed != (1, 12.5):
            wrong.append(f"[{label}] {observed}")
    assert not wrong, "\n".join(wrong)


def test_range_rules_compare_each_value_with_its_bounds_exactly(tmp_path: Path):
    wrong = []
    for column, values, expected in RULE_CASES:
        text = "\n".join([column, *values]) + "\n"
        path = tmp_path / f"{column}.csv"
        path.write_text(text, encoding="utf-8")
        sources: list[tuple[str, Any, dict[str, Any]]] = [
            ("bytes", text.encode(), {"format": "csv"}),
            *[
                (engine, str(path), {"engine": engine})
                for engine in ("auto", "incremental", "columnar")
            ],
        ]
        for label, source, options in sources:
            accuracy = dp.profile(source, **options).to_dict()["quality"]["accuracy"]
            observed = (
                accuracy["range_violations"],
                accuracy["rule_values_checked"],
                accuracy["rule_violating_values"],
            )
            if observed != expected:
                wrong.append(f"[{label}] {column}: {observed}")
    assert not wrong, "\n".join(wrong)
