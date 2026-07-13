"""Inspect a messy CSV the way you would on the first day with an unfamiliar export.

Run with:

    uv run python python/examples/messy_csv_inspection.py

The scenario: a partner sends you `orders.csv`. Nobody can tell you whether
`order_id` is really unique, whether `email` is populated, or why the finance
team's totals look wrong. One pass answers all three.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import dataprof as dp

# A realistic export: a null-heavy column, a duplicated key, a negative amount,
# a cell that fails to parse, and PII we should notice but never print.
MESSY_ORDERS = """\
order_id,customer_email,amount_eur,discount_code,shipped_at
1001,ada@example.com,49.90,,2026-01-04
1002,grace@example.com,120.00,,2026-01-04
1003,,15.50,SPRING,2026-01-05
1004,alan@example.com,-30.00,,2026-01-05
1005,,89.99,,2026-01-06
1006,edsger@example.com,not_available,SPRING,2026-01-06
1006,edsger@example.com,64.00,,2026-01-07
1008,,22.10,,2026-01-07
1009,barbara@example.com,310.00,SUMMER,2026-01-08
1010,,45.00,,2026-01-08
"""

# Pattern categories whose values must never reach a log, a ticket, or an LLM.
SENSITIVE_CATEGORIES = {"contact", "financial", "identifier"}


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "orders.csv"
        path.write_text(MESSY_ORDERS, encoding="utf-8")

        # `amount_eur` should never be negative, and `shipped_at` is the date
        # whose freshness we care about. Explicit hints turn both into quality
        # signals instead of relying on names or inferred types.
        report = dp.profile(
            str(path),
            positive_columns=["amount_eur"],
            identifier_columns=["order_id"],
            temporal_columns=["shipped_at"],
        )

        print(f"orders.csv: {report.rows} rows x {report.columns} columns\n")

        print(f"{'column':<16} {'type':<12} {'nulls':>6} {'distinct':>9}")
        print("-" * 46)
        for name, col in report.column_profiles.items():
            distinct = col.unique_count if col.unique_count is not None else "-"
            print(f"{name:<16} {col.data_type:<12} {col.null_percentage:>5.0f}% {distinct:>9}")

        print("\nwhat to worry about")
        print("-" * 46)

        for name, col in report.column_profiles.items():
            if col.null_percentage >= 20:
                print(
                    f"  {name}: {col.null_percentage:.0f}% missing -- optional, or a broken export?"
                )

            # `order_id` is declared an identifier, so anything short of fully
            # distinct is a duplicated key.
            present = col.total_count - col.null_count
            if name == "order_id" and col.unique_count is not None and col.unique_count < present:
                print(
                    f"  {name}: {present} rows but only {col.unique_count} distinct "
                    f"-- duplicate key"
                )

            # Report *that* we found PII, never the values themselves.
            for pattern in col.patterns or []:
                if pattern.category.lower() in SENSITIVE_CATEGORIES:
                    print(
                        f"  {name}: looks like {pattern.name} "
                        f"({pattern.match_percentage:.0f}% of rows) -- treat as sensitive"
                    )

            if name == "amount_eur" and col.min is not None and col.min < 0:
                print(f"  {name}: minimum is {col.min:.2f}, but amounts should never be negative")

        print(f"\noverall quality: {report.quality_score:.1f}/100")
        summary = report.quality_summary()
        for dimension in ("completeness", "consistency", "uniqueness", "accuracy", "timeliness"):
            print(f"  {dimension:<14} {summary[dimension]:.1f}")

        # A low completeness score is easy to misread. It scores *complete records*
        # -- rows with no null anywhere -- not the share of populated cells. Two
        # sparse columns are enough to make almost every row incomplete.
        quality = report.quality
        assert quality is not None and quality.completeness is not None
        completeness = quality.completeness
        print(
            f"\n  {completeness['complete_records_ratio']:.0f}% of rows are fully populated, "
            f"but only {completeness['missing_values_ratio']:.0f}% of cells are missing"
        )

        # `amount_eur` holds the literal `not_available`. Type inference tolerates
        # it, so the tell is the consistency score, not the reported data type.
        if summary["consistency"] < 100:
            print("  consistency < 100 means some cells do not match their column's type")


if __name__ == "__main__":
    main()
