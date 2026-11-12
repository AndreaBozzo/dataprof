"""A gate that notices a key column collapsing to a few values.

Run with:

    uv run --no-sync python python/examples/key_collapse_gate.py

The scenario: an orders extract is joined to a customer table before it
lands. One day the join breaks and fills `customer_id` with a default
value. Nothing is null, no row is duplicated and every cell still parses, so
the quality score does not move and the null limits pass. Only the number of
distinct customers gives it away: it falls from 40 to 1.

`max_distinct_drop` judges that number against the last accepted report. It
fails only when the distinct count fell by more than the allowance and its
share of the non-null values fell as far. A quiet day with half the rows and
half the customers keeps its share, so it passes.

Each report is stamped with `source_version`, the caller's name for the
extract. The gate result echoes it back, so the verdict says which two
extracts it compared.

The example runs three days, then the last one again without the distinct rule,
and prints every verdict. It exits 0 when each
run reaches the verdict the comments describe, and 1 otherwise. A real
pipeline exits non-zero on anything but a pass.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from typing import Any

import dataprof as dp


def orders(rows: int, customers: int, *, start: int = 1001, broken_join: bool = False) -> str:
    """One day's extract. A broken join gives every order the same customer."""
    lines = ["order_id,customer_id,amount_eur"]
    for i in range(rows):
        customer = "C-000" if broken_join else f"C-{100 + i % customers}"
        lines.append(f"O-{start + i},{customer},{10 + (i * 37) % 90}.50")
    return "\n".join(lines) + "\n"


# The absolute rules hold on every run. The broken day passes all of them.
ABSOLUTE: dict[str, Any] = {
    "min_quality_score": 85,
    "max_null_percentage": {"customer_id": 5},
    "max_duplicate_rows": 0,
}

# The rule about change. A customer count may fall by 10% of the baseline's
# before the gate objects, and then only if the distinct share fell as well.
CHANGE: dict[str, Any] = {
    "max_quality_score_drop": 2,
    "max_distinct_drop": {"customer_id": 10},
}


def judge(workdir: Path, label: str, contents: str, baseline: dp.ProfileReport) -> str:
    """Profile one day's extract and judge it against the baseline."""
    source = workdir / f"{label}.csv"
    source.write_text(contents, encoding="utf-8")
    report = dp.profile(str(source), identifier_columns=["order_id"], source_version=label)
    result = report.check(**ABSOLUTE, baseline=baseline, **CHANGE)

    distinct = report["customer_id"].unique_count
    print(f"{label}: {report.rows} rows, {distinct} distinct customers")
    print(f"  quality score {report.quality_score:.1f}, verdict: {result.verdict}")
    for check in result.checks:
        if check.code == "max_distinct_drop":
            seen = check.baseline["observed"] if check.baseline else None
            change = check.baseline["change"] if check.baseline else None
            print(
                f"  {check.status:<8} [{check.code}] {check.column}: {seen} -> {check.observed}",
                end="",
            )
            print(f" ({change:+.1f}%)" if change is not None else "")
    if result.baseline is not None:
        sides = result.baseline["reports"]
        today = sides["b"]["source_identity"]["version"]
        before = sides["a"]["source_identity"]["version"]
        print(f"  compared {today} with {before}")
    print()
    return result.verdict


# The verdict each run below must reach, in order. The last is Thursday again
# without the distinct rule.
EXPECTED = ("pass", "pass", "fail", "pass")


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        workdir = Path(tmp)

        # The accepted run: 200 orders from 40 customers.
        accepted = workdir / "monday.csv"
        accepted.write_text(orders(200, 40), encoding="utf-8")
        baseline = dp.profile(
            str(accepted), identifier_columns=["order_id"], source_version="monday"
        )
        kept = baseline["customer_id"].unique_count
        print(f"monday (baseline): {baseline.rows} rows, {kept} distinct customers")
        print()

        verdicts = []

        # 1. A quiet day: half the rows and half the customers. The distinct
        #    count fell by 50%, but the share of distinct values held at 20%,
        #    so this is a smaller extract, not a collapse. It passes.
        verdicts.append(judge(workdir, "tuesday", orders(100, 20, start=2001), baseline))

        # 2. A busier day with more customers. Counts that rise never fail.
        verdicts.append(judge(workdir, "wednesday", orders(300, 60, start=3001), baseline))

        # 3. The join breaks: 200 orders, one customer. The score is still
        #    100, nothing is null, and only max_distinct_drop fails.
        verdicts.append(
            judge(workdir, "thursday", orders(200, 40, start=4001, broken_join=True), baseline)
        )

        # Without the distinct rule, the same extract passes every other check.
        broken = dp.profile(str(workdir / "thursday.csv"), identifier_columns=["order_id"])
        others = {k: v for k, v in CHANGE.items() if k != "max_distinct_drop"}
        verdict = broken.check(**ABSOLUTE, baseline=baseline, **others).verdict
        print(f"thursday without max_distinct_drop: {verdict}")
        verdicts.append(verdict)

    if tuple(verdicts) != EXPECTED:
        print(f"unexpected verdicts: {verdicts}, expected {list(EXPECTED)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
