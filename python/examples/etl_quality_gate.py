"""Accept or reject an incoming dataset before it reaches your warehouse.

Run with:

    uv run python python/examples/etl_quality_gate.py

The scenario: a daily drop lands in a staging bucket. You want the pipeline to
stop on the bad file rather than propagate it downstream, and you want the
rejection reason in the logs. The gate below is a plain function over a
`ProfileReport`, so it composes into Airflow, Dagster, or a shell script.

This example profiles a good file and a bad one and prints both verdicts, so it
always exits 0. A real gate would `sys.exit(1)` on rejection.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

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

# What the warehouse is willing to accept.
MIN_QUALITY_SCORE = 90.0
REQUIRED_COLUMNS = ("transaction_id", "account", "amount_eur", "booked_at")
KEY_COLUMN = "transaction_id"
POSITIVE_COLUMNS = ["amount_eur"]
MAX_MISSING_PERCENTAGE = 5.0


def violations(report: dp.ProfileReport) -> list[str]:
    """Return every reason this dataset must not be loaded. Empty means "accept".

    Each check answers a question an on-call engineer would actually ask, and the
    message names the column, so the pipeline log is enough to triage without
    re-running the profiler.
    """
    reasons: list[str] = []
    columns = report.column_profiles

    for required in REQUIRED_COLUMNS:
        if required not in columns:
            reasons.append(f"missing required column `{required}`")

    quality = report.quality
    if report.quality_score is None or quality is None or quality.completeness is None:
        return [*reasons, "quality assessment was skipped"]

    if report.quality_score < MIN_QUALITY_SCORE:
        reasons.append(
            f"quality score {report.quality_score:.1f} is below "
            f"the {MIN_QUALITY_SCORE:.1f} threshold"
        )

    completeness = quality.completeness
    # `missing_values_ratio` is reported as a percentage, not a 0..1 fraction.
    if completeness["missing_values_ratio"] > MAX_MISSING_PERCENTAGE:
        reasons.append(
            f"{completeness['missing_values_ratio']:.1f}% of cells are missing, "
            f"above the {MAX_MISSING_PERCENTAGE:.1f}% allowance"
        )
    # `null_columns` lists columns past the configured null threshold (50% by
    # default), not only columns that are entirely null.
    for null_column in completeness["null_columns"]:
        reasons.append(f"column `{null_column}` is mostly null")

    # A key that repeats means the upstream job double-wrote, or we are about to
    # create duplicates on load. Either way, do not proceed.
    key = columns.get(KEY_COLUMN)
    if key is not None:
        present = key.total_count - key.null_count
        if key.unique_count is not None and key.unique_count < present:
            reasons.append(
                f"key `{KEY_COLUMN}` has {present} values but only {key.unique_count} distinct"
            )
        if key.null_count:
            reasons.append(f"key `{KEY_COLUMN}` has {key.null_count} null value(s)")

    accuracy = quality.accuracy or {}
    negatives = accuracy.get("negative_values_in_positive")
    if negatives:
        reasons.append(f"{negatives} negative value(s) in {POSITIVE_COLUMNS}")

    return reasons


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        for label, contents in (("good_drop.csv", GOOD_DROP), ("bad_drop.csv", BAD_DROP)):
            path = Path(tmp) / label
            path.write_text(contents, encoding="utf-8")

            report = dp.profile(
                str(path),
                positive_columns=POSITIVE_COLUMNS,
                identifier_columns=[KEY_COLUMN],
            )
            reasons = violations(report)

            print(label)
            if not reasons:
                print(f"  ACCEPT -- quality {report.quality_score:.1f}/100\n")
                continue

            print(f"  REJECT -- {len(reasons)} violation(s):")
            for reason in reasons:
                print(f"    - {reason}")
            print("  a real pipeline would sys.exit(1) here\n")


if __name__ == "__main__":
    main()
