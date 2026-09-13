"""Accept or reject an incoming dataset before it reaches your warehouse.

Run with:

    uv run python python/examples/etl_quality_gate.py

The scenario: a daily drop lands in a staging bucket. You want the pipeline to
stop on the bad file rather than propagate it downstream, and you want the
rejection reason in the logs. `ProfileReport.check()` states the policy as data
and returns a structured result, so the gate composes into Airflow, Dagster, or
a shell script without a CLI.

This example profiles four drops and prints all three verdicts, so it always
exits 0. A real gate would `sys.exit(1)` on anything but a pass.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

import dataprof as dp

GOOD_DROP = """\
transaction_id,account,amount_eur,booked_at
T-1001,ACC-1,120.00,2026-01-04
T-1002,ACC-2,88.50,2026-01-04
T-1003,ACC-1,45.25,2026-01-05
T-1004,ACC-3,310.00,2026-01-05
T-1005,ACC-2,17.99,2026-01-06
"""

BAD_DROP = """\
transaction_id,account,amount_eur,booked_at
T-2001,ACC-1,120.00,2026-01-04
T-2002,,88.50,2026-01-04
T-2002,ACC-1,-45.25,2026-01-05
T-2004,,,2026-01-05
T-2005,,17.99,2026-01-06
"""

# Same rows as the good drop, but the last one repeats the first. A capped scan
# still sees it, which is what makes the third verdict interesting.
DUPLICATED_DROP = GOOD_DROP + "T-1001,ACC-1,120.00,2026-01-04\n"

KEY_COLUMN = "transaction_id"
REQUIRED_COLUMNS = ("transaction_id", "account", "amount_eur", "booked_at")

# What the warehouse is willing to accept. Every threshold is a 0-100
# percentage, matching what the report reports.
POLICY: dict[str, Any] = {
    "min_quality_score": 90,
    "max_null_percentage": {KEY_COLUMN: 0, "*": 5},
    "max_duplicate_rows": 0,
    # Without this, a run that never computed quality would leave the score
    # check unevaluated rather than failing. A pipeline wants the
    # misconfiguration to be loud.
    "require_metrics": ["quality", "completeness"],
}


def missing_columns(report: dp.ProfileReport) -> list[str]:
    """Schema presence, which the numeric policy deliberately does not cover.

    A column absent from a report was either projected away or not in the
    source, and the report does not record which -- so `check()` leaves it
    undecided. Here the run profiles everything, so absence means absence.
    """
    return [name for name in REQUIRED_COLUMNS if name not in report]


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        # The fourth drop is the third one read under a row cap: the duplicate
        # sits past the cap, so the scan never witnesses it. The policy asks
        # about the whole source, and a clean prefix is not a clean source.
        drops = (
            ("good_drop.csv", GOOD_DROP, None),
            ("bad_drop.csv", BAD_DROP, None),
            ("duplicated_drop.csv", DUPLICATED_DROP, None),
            ("capped_scan.csv", DUPLICATED_DROP, 3),
        )
        for label, contents, max_rows in drops:
            path = Path(tmp) / label
            path.write_text(contents, encoding="utf-8")

            report = dp.profile(
                str(path),
                identifier_columns=[KEY_COLUMN],
                stop_condition=None if max_rows is None else dp.StopCondition.max_rows(max_rows),
            )
            result = report.check(**POLICY)

            print(label)
            for name in missing_columns(report):
                print(f"  REJECT -- missing required column `{name}`")

            # Three verdicts, not two. "inconclusive" means nothing was
            # violated and something could not be checked -- an unanalyzed
            # metric, or a scan that did not reach as far as the policy asks.
            # Treating it as a pass is exactly the mistake to avoid.
            if result.verdict == "pass":
                print(f"  ACCEPT -- quality {report.quality_score}/100\n")
                continue

            outcome = "REJECT" if result.verdict == "fail" else "HOLD"
            reported = result.violations or result.unevaluated
            print(f"  {outcome} -- {len(reported)} finding(s):")
            for check in reported:
                where = check.column or check.dimension or "report"
                print(f"    - [{check.code}] {where}: {check.message}")
                if check.observed is not None:
                    print(f"        observed {check.observed}, expected {check.expected}")
            print("  a real pipeline would sys.exit(1) here\n")


if __name__ == "__main__":
    main()
