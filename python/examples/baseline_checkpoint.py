"""A quality checkpoint that judges each drop against the last accepted one.

Run with:

    uv run python python/examples/baseline_checkpoint.py

The scenario: an orders extract lands every day. Absolute thresholds catch a
broken file, but not a slow slide: a key column that goes from 0% to 15%
empty still passes a 20% limit. So each run also compares the drop with the
last report the pipeline accepted, its baseline, and judges how far it moved.

Three files carry the state, and the rules for them are the point of this
example:

- `baseline.json` is the last accepted report. Only a passing run replaces
  it, and only by renaming the candidate onto it; a failing or inconclusive
  run leaves it as it was.
- `candidate.json` is today's report, written whatever the verdict, so the
  evidence of a rejected drop stays available beside the verdict.
- The policy is data. Rules about change need a baseline, so the first run,
  with none yet, applies the absolute rules alone.

This example runs seven drops and prints every verdict. It exits 0 when each
drop reaches the verdict the comments below describe, and 1 otherwise, so CI
notices when the story stops being true. A real pipeline exits non-zero on
anything but a pass; `python -m dataprof.check` does, with exit codes 0 pass,
1 fail, 2 inconclusive.
"""

from __future__ import annotations

import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

import dataprof as dp

HEADER = "order_id,customer_id,amount_eur,status\n"
STATUSES = ("paid", "shipped", "pending", "paid", "shipped")


def orders(
    count: int, *, start: int = 1001, missing_customers: int = 0, coupon: bool = False
) -> str:
    """A day's extract. The first `missing_customers` rows lack a customer."""
    header = HEADER.replace("\n", ",coupon_code\n") if coupon else HEADER
    rows = []
    for i in range(count):
        customer = "" if i < missing_customers else f"C-{100 + i % 7}"
        row = f"O-{start + i},{customer},{10 + (i * 37) % 90}.50,{STATUSES[i % 5]}"
        if coupon:
            row += "," + ("SPRING" if i % 4 == 0 else "")
        rows.append(row)
    return header + "\n".join(rows) + "\n"


# Absolute rules: they hold on every run, the first included.
ABSOLUTE: dict[str, Any] = {
    "min_quality_score": 85,
    "max_null_percentage": {"order_id": 0, "customer_id": 20},
    "max_duplicate_rows": 0,
    "require_columns": ["order_id", "customer_id", "amount_eur", "status"],
}

# Rules about change: they need a baseline, so they apply once a run has been
# accepted.
CHANGE: dict[str, Any] = {
    "max_quality_score_drop": 2,
    "max_null_percentage_increase": {"*": 5},
    "forbid_removed_columns": True,
}

# The baseline 0.12.0 wrote for the third drop's data, committed beside this
# example: `orders(20, start=2001)` saved as `day3_clean.csv`, then
# `dp.profile("day3_clean.csv", identifier_columns=["order_id"]).save(...)`
# with the released 0.12.0 wheel. That release did not record how it read null
# markers, so a rule about change cannot tell a difference in the data from one
# in the definition.
LEGACY_BASELINE = Path(__file__).parent / "data" / "orders_baseline_dataprof_0_12_0.json"


def evaluate(workdir: Path, label: str, contents: str, **profile_options: Any) -> str:
    """One checkpoint run: profile, judge, keep the evidence, promote on a pass."""
    source = workdir / f"{label}.csv"
    source.write_text(contents, encoding="utf-8")
    baseline_path = workdir / "baseline.json"
    candidate_path = workdir / "candidate.json"
    accepted_before = baseline_path.read_bytes() if baseline_path.exists() else None

    report = dp.profile(str(source), identifier_columns=["order_id"], **profile_options)
    report.save(str(candidate_path))

    if baseline_path.exists():
        baseline = dp.ProfileReport.load(str(baseline_path))
        result = report.check(**ABSOLUTE, baseline=baseline, **CHANGE)
        against = "against the accepted baseline"
    else:
        result = report.check(**ABSOLUTE)
        against = "no baseline yet: absolute rules only"

    print(f"{label} ({against})")
    show(result)
    if result.verdict == "pass":
        # The promotion: a rename, so the baseline is always a complete report
        # that passed. Nothing else in this file ever writes it.
        candidate_path.replace(baseline_path)
        print("  ACCEPT -- candidate promoted to baseline\n")
    else:
        kept = "unchanged" if baseline_path.exists() else "still absent"
        print(f"  {'REJECT' if result.verdict == 'fail' else 'HOLD'} -- baseline {kept},")
        print("    candidate.json kept beside the verdict until the next run; a real")
        print("    pipeline exits non-zero and stores both with the run\n")
        # What the rules above promise, checked: the accepted baseline is
        # exactly as it was (or still absent), and the evidence is kept.
        accepted_after = baseline_path.read_bytes() if baseline_path.exists() else None
        if accepted_after != accepted_before or not candidate_path.exists():
            raise RuntimeError(f"{label}: a {result.verdict} run changed the accepted baseline")
    return result.verdict


def show(result: dp.QualityGateResult) -> None:
    """The verdict and what decided it: failures and the checks that could not look."""
    print(f"  verdict: {result.verdict}")
    for check in [*result.violations, *result.unevaluated]:
        where = check.column or check.dimension or "report"
        line = f"    {check.status:<12} [{check.code}] {where}"
        if check.baseline is not None and check.baseline.get("change") is not None:
            line += f": changed {check.baseline['change']:+} points"
        elif check.observed is not None:
            line += f": observed {check.observed}"
        if check.reason is not None:
            line += f" -- {check.reason['reason']}"
            # Which definition or setting the two reports disagree on, if any.
            subject = check.reason.get("definition") or check.reason.get("setting")
            if subject:
                line += f" ({subject})"
        print(line)
    if result.baseline is not None:
        for diagnostic in result.baseline["diagnostics"]:
            print(f"    diagnostic   {diagnostic['code']} {diagnostic.get('column', '')}")


# The verdict each drop below must reach, in order.
EXPECTED = ("fail", "pass", "pass", "fail", "inconclusive", "inconclusive", "inconclusive")


def main() -> int:
    verdicts = []
    with tempfile.TemporaryDirectory() as tmp:
        workdir = Path(tmp)

        # 1. The very first drop is broken. A failing first run must not become
        #    the baseline, or every later run is judged against bad data.
        verdicts.append(evaluate(workdir, "day1_broken", orders(20, missing_customers=6)))

        # 2. A clean drop with still no baseline: absolute rules decide, and a
        #    pass creates the first baseline.
        verdicts.append(evaluate(workdir, "day2_clean", orders(20)))

        # 3. From now on each run is also judged against the last accepted one.
        verdicts.append(evaluate(workdir, "day3_clean", orders(20, start=2001)))

        # 4. A slow slide: customer_id goes from 0% to 15% empty. The absolute
        #    limit is 20% and the score stays above 85, so only the rules about
        #    change see it.
        verdicts.append(
            evaluate(workdir, "day4_slide", orders(20, start=3001, missing_customers=3))
        )

        # 5. A new column appears. The score now measures a different column
        #    set, so a drop in it is not evidence of worse data: the score rule
        #    is unevaluated (schema_changed) and the run is inconclusive. The
        #    per-column rules still decide.
        verdicts.append(evaluate(workdir, "day5_new_column", orders(20, start=4001, coupon=True)))
        if verdicts[-1] == "inconclusive":
            # Accepting the new shape is a decision, made by a person who has
            # looked at the candidate. In a repository this is a reviewed pull
            # request that commits candidate.json as the new baseline.
            (workdir / "candidate.json").replace(workdir / "baseline.json")
            print("  REVIEWED -- coupon_code accepted; candidate committed as baseline\n")

        # 6. A run that did not analyze quality at all. Every rule that reads a
        #    score cannot look, so each is unevaluated (quality_unavailable)
        #    instead of passing. `require_metrics=["quality"]` in the policy
        #    would make this a failure; leaving it inconclusive is a choice.
        verdicts.append(
            evaluate(
                workdir,
                "day6_no_quality",
                orders(20, start=5001, coupon=True),
                metrics=["schema", "statistics"],
            )
        )

        # 7. The third drop's data again, against the baseline 0.12.0 wrote for
        #    it. The numbers have not moved, yet each rule about change is
        #    unevaluated: 0.12.0 did not record how it read null tokens, so a
        #    zero change is not evidence. The absolute rules still decide, and
        #    the way forward is to re-baseline on the current release on purpose.
        shutil.copy(LEGACY_BASELINE, workdir / "baseline.json")
        verdicts.append(evaluate(workdir, "day7_old_baseline", orders(20, start=2001)))

    if tuple(verdicts) != EXPECTED:
        print(f"unexpected verdicts: {verdicts}, expected {list(EXPECTED)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
