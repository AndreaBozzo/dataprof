# /// script
# requires-python = ">=3.11,<3.15"
# dependencies = ["dataprof>=0.14.1", "dlt[duckdb,parquet]==1.32.0"]
# ///
# dlt 1.32.0 supports Python 3.10–3.14. dataprof 0.14.1 starts at 3.11.
"""Run a pre-load quality checkpoint with uv run examples/dlt_preload_checkpoint.py."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

import dataprof as dp
import pyarrow as pa

if TYPE_CHECKING:
    import dlt


def orders(start: int, count: int) -> pa.Table:
    return pa.table({"order_id": range(start, start + count), "amount_cents": [100] * count})


def check_and_load(
    pipeline: dlt.Pipeline,
    batch: pa.Table,
    *,
    baseline: dp.ProfileReport | None = None,
    max_rows: int | None = None,
) -> tuple[dp.ProfileReport, dp.QualityGateResult]:
    report = dp.profile(batch, max_rows=max_rows)
    result = report.check(
        max_null_count={"order_id": 0},
        baseline=baseline,
        allow_missing_baseline=True,
        max_row_count_drop=25,
        scope="full_source",
    )
    if result.verdict == "pass":
        pipeline.run(batch, table_name="orders", write_disposition="replace").raise_on_failed_jobs()
    return report, result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario", choices=("healthy", "short-batch", "partial-scan"), default="healthy"
    )
    scenario = parser.parse_args().scenario

    os.environ["RUNTIME__DLTHUB_TELEMETRY"] = "false"
    import dlt

    with TemporaryDirectory(prefix="dataprof-dlt-preload-") as tmp:
        workdir = Path(tmp)
        pipeline = dlt.pipeline(
            pipeline_name="orders_preload_checkpoint",
            pipelines_dir=str(workdir / "pipelines"),
            destination=dlt.destinations.duckdb(str(workdir / "orders.duckdb")),
            dataset_name="staging",
        )
        baseline, first = check_and_load(pipeline, orders(1, 4))
        if first.verdict != "pass":
            raise RuntimeError(f"Generated baseline did not pass:\n{first}")

        report, result = check_and_load(
            pipeline,
            orders(101, 2 if scenario == "short-batch" else 4),
            baseline=baseline,
            max_rows=2 if scenario == "partial-scan" else None,
        )
        loaded = pipeline.dataset().table("orders").select("order_id").arrow()
        loaded_ids = sorted(loaded.column("order_id").to_pylist())

    print(
        f"verdict: {result.verdict} (baseline rows: {baseline.rows}, rows profiled: {report.rows})"
    )
    print(f"DuckDB staging.orders IDs: {loaded_ids}")
    for check in [*result.violations, *result.unevaluated]:
        print(f"  {check.code}: {check.message}")

    if result.verdict == "fail":
        print("Candidate held before load because it violated the policy.")
        return 1
    if result.verdict == "inconclusive":
        print("Candidate held before load until a full scan can establish the row count.")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
