"""The range and sign rules of the accuracy score count values of the columns
they apply to, each value once (#1005).

The failures used to be divided by the numeric values of every column, so
clean columns beside a broken one diluted its penalty, and a value breaking
two rules (a negative percentage hinted positive) counted twice.

``tests/accuracy_rule_units.rs`` is the Rust twin.
"""

from __future__ import annotations

import importlib.util
import json
from collections.abc import Callable
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

#: A percentage column hinted positive: -10, 0, 10, ..., 180. Evenly spaced,
#: so no value is a Tukey outlier. -10 breaks the percentage and the sign rule,
#: 110 to 180 the percentage rule: 9 violating values of 20.
PERCENT = "discount_percent"
PERCENT_VALUES = [row * 10 - 10 for row in range(20)]

#: Clean numeric columns under no rule: 1 to 20.
CLEAN = {f"reading_{index}": list(range(1, 21)) for index in range(1, 10)}

Table = dict[str, list[int]]
Route = Callable[[Table], Any]


def _routes(tmp_path: Path) -> dict[str, Route]:
    """Every input route, each profiling a table with `PERCENT` hinted positive."""

    def run(source: Any, **options: Any) -> Any:
        return dp.profile(source, positive_columns=[PERCENT], **options)

    def rows(table: Table) -> list[dict[str, int]]:
        return [
            dict(zip(table, values, strict=True)) for values in zip(*table.values(), strict=True)
        ]

    def csv_file(engine: str) -> Route:
        def route(table: Table) -> Any:
            path = tmp_path / f"{engine}-{len(table)}.csv"
            lines = [",".join(table), *(",".join(map(str, row.values())) for row in rows(table))]
            path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            return run(str(path), engine=engine)

        return route

    def json_file(table: Table) -> Any:
        path = tmp_path / f"table-{len(table)}.json"
        path.write_text(json.dumps(rows(table)), encoding="utf-8")
        return run(str(path))

    def jsonl_file(table: Table) -> Any:
        path = tmp_path / f"table-{len(table)}.jsonl"
        path.write_text("\n".join(json.dumps(row) for row in rows(table)) + "\n", encoding="utf-8")
        return run(str(path))

    def arrow_table(table: Table) -> Any:
        import pyarrow as pa

        return pa.table({name: pa.array(values, pa.int64()) for name, values in table.items()})

    def parquet_file(table: Table) -> Any:
        import pyarrow.parquet as pq

        path = tmp_path / f"table-{len(table)}.parquet"
        pq.write_table(arrow_table(table), path)
        return run(str(path))

    def pandas(table: Table) -> Any:
        import pandas as pd

        return run(pd.DataFrame(table))

    def polars(table: Table) -> Any:
        import polars as pl

        return run(pl.DataFrame(table))

    routes: dict[str, Route] = {
        "auto": csv_file("auto"),
        "incremental": csv_file("incremental"),
        "columnar": csv_file("columnar"),
        "json": json_file,
        "jsonl": jsonl_file,
        "dict": run,
        "rows": lambda table: run(rows(table)),
    }
    # An optional library drops only its own routes; skipping inside a route
    # would skip the whole test and every route it had not reached yet.
    optional = {
        "parquet": ("pyarrow", parquet_file),
        "arrow": ("pyarrow", lambda table: run(arrow_table(table))),
        "pandas": ("pandas", pandas),
        "polars": ("polars", polars),
    }
    for label, (module, route) in optional.items():
        if importlib.util.find_spec(module) is not None:
            routes[label] = route
    return routes


def test_clean_columns_and_overlapping_rules_leave_the_rule_component_alone(tmp_path: Path):
    wrong = []
    for with_clean in (False, True):
        table = {PERCENT: PERCENT_VALUES, **(CLEAN if with_clean else {})}
        expected = {
            "range_violations": 9,
            "negative_values_in_positive": 1,
            "numeric_values_checked": 200 if with_clean else 20,
            "rule_values_checked": 20,
            "rule_violating_values": 9,
        }
        for label, route in _routes(tmp_path).items():
            report = route(table)
            quality = report.to_dict()["quality"]
            where = f"[{label}, clean columns: {with_clean}]"
            for field, value in expected.items():
                if quality["accuracy"].get(field) != value:
                    wrong.append(
                        f"{where} {field} {quality['accuracy'].get(field)}, expected {value}"
                    )
            if report.quality.accuracy != quality["accuracy"]:
                wrong.append(f"{where} accessor {report.quality.accuracy} != {quality['accuracy']}")
            # 9 violating values of 20, no outliers: 100 - 45.
            if quality["dimension_scores"]["accuracy"] != 55.0:
                wrong.append(
                    f"{where} accuracy {quality['dimension_scores']['accuracy']}, expected 55.0"
                )
            if report.metric_semantics.get("range_rule_count") != "violating_values":
                wrong.append(f"{where} metric_semantics {report.metric_semantics}")
    assert not wrong, "\n".join(wrong)
