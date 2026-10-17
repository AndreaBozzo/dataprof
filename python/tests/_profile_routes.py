"""Profile a string column over each supported input route."""

from __future__ import annotations

import csv
import importlib.util
import io
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import dataprof as dp

NAME = "event"
Route = Callable[[str, list[str]], Any]


def _routes(tmp_path: Path, **kwargs: Any) -> dict[str, Route]:
    """Every input route, each profiling one string column."""

    def run_profile(source: Any, **options: Any) -> Any:
        return dp.profile(source, **kwargs, **options)

    def csv_bytes(values: list[str]) -> bytes:
        # Quoted where needed, so a value holding a comma stays one cell.
        buffer = io.StringIO(newline="")
        csv.writer(buffer, lineterminator="\n").writerows([[NAME], *([v] for v in values)])
        return buffer.getvalue().encode()

    def csv_file(engine: str) -> Route:
        def run(label: str, values: list[str]) -> Any:
            path = tmp_path / f"{label}-{engine}.csv"
            path.write_bytes(csv_bytes(values))
            return run_profile(str(path), engine=engine)

        return run

    def json_file(label: str, values: list[str]) -> Any:
        path = tmp_path / f"{label}.json"
        path.write_text(json.dumps([{NAME: v} for v in values]), encoding="utf-8")
        return run_profile(str(path))

    def jsonl_file(label: str, values: list[str]) -> Any:
        path = tmp_path / f"{label}.jsonl"
        path.write_text("\n".join(json.dumps({NAME: v}) for v in values) + "\n", encoding="utf-8")
        return run_profile(str(path))

    def parquet_file(label: str, values: list[str]) -> Any:
        import pyarrow as pa
        import pyarrow.parquet as pq

        path = tmp_path / f"{label}.parquet"
        pq.write_table(pa.table({NAME: pa.array(values, pa.string())}), path)
        return run_profile(str(path))

    def arrow(label: str, values: list[str]) -> Any:
        import pyarrow as pa

        return run_profile(pa.table({NAME: pa.array(values, pa.string())}))

    def arrow_stream(label: str, values: list[str]) -> Any:
        import pyarrow as pa

        return run_profile(pa.table({NAME: pa.array(values, pa.string())}).to_reader())

    def duckdb(label: str, values: list[str]) -> Any:
        duckdb = importlib.import_module("duckdb")
        import pyarrow as pa

        return run_profile(duckdb.from_arrow(pa.table({NAME: pa.array(values, pa.string())})))

    def pandas(label: str, values: list[str]) -> Any:
        import pandas as pd

        return run_profile(pd.DataFrame({NAME: values}))

    def polars(label: str, values: list[str]) -> Any:
        import polars as pl

        return run_profile(pl.DataFrame({NAME: values}))

    routes: dict[str, Route] = {
        "auto": csv_file("auto"),
        "incremental": csv_file("incremental"),
        "columnar": csv_file("columnar"),
        "bytes": lambda label, values: run_profile(csv_bytes(values), format="csv"),
        "json": json_file,
        "jsonl": jsonl_file,
        "dict": lambda label, values: run_profile({NAME: values}),
        "rows": lambda label, values: run_profile([{NAME: v} for v in values]),
    }
    # An optional library drops only its own routes; skipping inside a route
    # would skip the whole test and every route it had not reached yet.
    optional = {"parquet": ("pyarrow", parquet_file), "arrow": ("pyarrow", arrow)}
    optional["arrow_stream"] = ("pyarrow", arrow_stream)
    optional |= {"pandas": ("pandas", pandas), "polars": ("polars", polars)}
    for label, (module, route) in optional.items():
        if importlib.util.find_spec(module) is not None:
            routes[label] = route
    if all(importlib.util.find_spec(module) is not None for module in ("duckdb", "pyarrow")):
        routes["duckdb"] = duckdb
    return routes
