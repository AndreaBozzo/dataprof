"""Prove that a cleaning step actually improved the data.

Run with:

    uv run python python/examples/before_after_cleaning.py

The scenario: you wrote a cleaning script. Did it help? Profile the raw file,
persist that report, run the cleaning, profile the result, and diff the two.
Persisting the "before" report matters: weeks later you can still answer "what
did this data look like when we onboarded it?" without keeping the raw file.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import dataprof as dp

RAW = """\
signup_id,email,plan,monthly_eur
S-1,ada@example.com,pro,29.00
S-2,,free,0.00
S-3,grace@example.com,PRO,29.00
S-4,,,
S-5,alan@example.com,free,0.00
S-5,alan@example.com,free,0.00
S-7,,enterprise,-99.00
S-8,edsger@example.com,pro,29.00
"""

# What our (imaginary) cleaning script does: drop rows with no email, drop the
# duplicated signup, normalise `plan` casing, and reject the negative price.
CLEAN = """\
signup_id,email,plan,monthly_eur
S-1,ada@example.com,pro,29.00
S-3,grace@example.com,pro,29.00
S-5,alan@example.com,free,0.00
S-8,edsger@example.com,pro,29.00
"""


def profile(path: Path) -> dp.ProfileReport:
    return dp.profile(
        str(path),
        positive_columns=["monthly_eur"],
        identifier_columns=["signup_id"],
    )


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        raw_path = tmp_path / "signups_raw.csv"
        clean_path = tmp_path / "signups_clean.csv"
        raw_path.write_text(RAW, encoding="utf-8")
        clean_path.write_text(CLEAN, encoding="utf-8")

        # Persist the baseline before touching the data. A saved report is plain
        # JSON: commit it, ship it to object storage, or attach it to a PR.
        baseline = tmp_path / "baseline.json"
        profile(raw_path).save(str(baseline))

        before = dp.ProfileReport.load(str(baseline))
        after = profile(clean_path)

        before_score = before.quality_score
        after_score = after.quality_score
        assert before_score is not None and after_score is not None

        print("## Cleaning summary\n")
        print("| | rows | quality |")
        print("|---|---:|---:|")
        print(f"| before | {before.rows} | {before_score:.1f} |")
        print(f"| after | {after.rows} | {after_score:.1f} |")

        dropped = before.rows - after.rows
        gained = after_score - before_score
        print(f"\n{dropped} row(s) dropped, {gained:+.1f} quality points\n")

        # `compare()` does the diffing for you: quality, schema, and per-column
        # deltas. `a` is this report, `b` is the other one, and `abs` is `b - a`
        # (signed, despite the name).
        delta = before.compare(after)
        print("## What changed\n")
        print(f"quality_score: {delta['quality_score']['abs']:+.1f}")
        for dimension, change in sorted(delta["dimensions"].items()):
            if change["abs"]:
                print(f"  {dimension:<14} {change['abs']:+.1f}")

        if delta["schema"]["removed"]:
            print(f"\ncolumns dropped by cleaning: {delta['schema']['removed']}")

        print("\n## Per-column null rate\n")
        print("| column | before | after | delta |")
        print("|---|---:|---:|---:|")
        for name, change in sorted(delta["columns"].items()):
            print(
                f"| {name} | {change['null_pct_a']:.0f}% | {change['null_pct_b']:.0f}% "
                f"| {change['null_pct_delta']:+.0f}pp |"
            )

        # The headline numbers can improve while a specific defect survives. Check
        # the defects you set out to fix, not just the score.
        print("\n## Did the defects go away?\n")
        columns = after.column_profiles
        signup_id = columns["signup_id"]
        quality = after.quality
        assert quality is not None and quality.accuracy is not None
        checks = {
            "duplicate signup_id": (
                signup_id.unique_count is not None
                and signup_id.unique_count < signup_id.total_count - signup_id.null_count
            ),
            "negative monthly_eur": quality.accuracy["negative_values_in_positive"] > 0,
            "missing email": columns["email"].null_count > 0,
        }
        for defect, still_present in checks.items():
            print(f"  {'STILL THERE' if still_present else 'fixed      '} {defect}")


if __name__ == "__main__":
    main()
