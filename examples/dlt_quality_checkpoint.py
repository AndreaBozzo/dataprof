# /// script
# requires-python = ">=3.11,<3.15"
# dependencies = ["dataprof>=0.14.1", "dlt[duckdb,parquet]==1.32.0"]
# ///
# dlt 1.32.0 supports Python 3.10–3.14; dataprof 0.14.1 starts at 3.11.
"""Run a post-load quality checkpoint with uv run examples/dlt_quality_checkpoint.py.

To check the Arrow batch before loading it, run uv run examples/dlt_preload_checkpoint.py.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

import dataprof as dp

if TYPE_CHECKING:
    import dlt


def load_and_profile(
    pipeline: dlt.Pipeline, rows: int, *, max_rows: int | None = None
) -> dp.ProfileReport:
    orders = [{"order_id": i, "amount_cents": i * 100} for i in range(1, rows + 1)]
    pipeline.run(orders, table_name="orders", write_disposition="replace").raise_on_failed_jobs()
    table = pipeline.dataset().table("orders").select("order_id", "amount_cents").arrow()
    return dp.profile(table, max_rows=max_rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario", choices=("healthy", "short-load", "partial-scan"), default="healthy"
    )
    scenario = parser.parse_args().scenario

    # Set this before dlt initializes its runtime and configuration providers.
    os.environ["RUNTIME__DLTHUB_TELEMETRY"] = "false"
    import dlt

    with TemporaryDirectory(prefix="dataprof-dlt-") as tmp:
        workdir = Path(tmp)
        pipeline = dlt.pipeline(
            pipeline_name="orders_checkpoint",
            pipelines_dir=str(workdir / "pipelines"),
            destination=dlt.destinations.duckdb(str(workdir / "orders.duckdb")),
            dataset_name="staging",
        )
        baseline_path = workdir / "baseline.json"
        load_and_profile(pipeline, 4).save(baseline_path)
        baseline = dp.ProfileReport.load(baseline_path)
        report = load_and_profile(
            pipeline,
            2 if scenario == "short-load" else 4,
            max_rows=2 if scenario == "partial-scan" else None,
        )
        result = report.check(
            max_null_count={"order_id": 0},
            baseline=baseline,
            max_row_count_drop=25,
            scope="full_source",
        )

    print(
        f"verdict: {result.verdict} (baseline rows: {baseline.rows}, rows profiled: {report.rows})"
    )
    for check in [*result.violations, *result.unevaluated]:
        print(f"  {check.code}: observed={check.observed}, reason={check.reason}")

    if result.verdict == "fail":
        return 1
    if result.verdict == "inconclusive":
        # A partial scan cannot establish the loaded batch's row count.
        # Stop downstream work until the full dataset can be checked.
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
