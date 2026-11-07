"""A flat summary without ``execution.sampling_applied`` is unrecorded coverage (#1236).

``from_dict()`` keeps the ``sampling_applied`` accessor's ``False`` default,
but the summary it re-exports, which is the document ``compare()`` and
``check(baseline=)`` read, omits the field it was not given. A missing field is
then ``coverage_unrecorded`` and the gate is inconclusive, as in the Rust
reader; a recorded ``false`` decides and a recorded ``true`` names the
sampling. The ``python -m dataprof.check`` entrypoint reads a saved baseline the
same way.

``tests/summary_sampling_applied.rs`` is the Rust twin.
"""

from __future__ import annotations

import json
import subprocess
import sys
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

_EXIT_CODES = {"pass": 0, "fail": 1, "inconclusive": 2}

# case -> (recorded value or None for missing, evidence, verdict)
CASES: dict[str, tuple[bool | None, dict[str, str], str]] = {
    "missing": (
        None,
        {"coverage": "incomplete", "reason": "coverage_unrecorded"},
        "inconclusive",
    ),
    "false": (False, {"coverage": "complete"}, "pass"),
    "true": (True, {"coverage": "incomplete", "reason": "sampled"}, "inconclusive"),
}


def _summary(recorded: bool | None) -> dict[str, Any]:
    summary = dp.profile({"x": [1, 2, 3, 4]}).to_dict()
    assert summary["execution"]["sampling_applied"] is False
    if recorded is None:
        del summary["execution"]["sampling_applied"]
    else:
        summary["execution"]["sampling_applied"] = recorded
    return summary


def test_a_summary_baseline_decides_only_on_a_recorded_sampling_applied(tmp_path: Path) -> None:
    source = tmp_path / "batch.csv"
    source.write_text("x\n1\n2\n3\n4\n", encoding="utf-8")
    current = dp.profile_file(source)
    wrong = []
    for case, (recorded, evidence, verdict) in CASES.items():
        summary = _summary(recorded)
        baseline = dp.ProfileReport.from_dict(summary)
        # The accessor keeps its historical default.
        if baseline.sampling_applied is not bool(recorded):
            wrong.append((case, "accessor", baseline.sampling_applied))
        resaved = baseline.to_dict()["execution"].get("sampling_applied", "absent")
        if resaved != ("absent" if recorded is None else recorded):
            wrong.append((case, "resaved summary", resaved))
        observed = baseline.compare(current)["reports"]["a"]["evidence"]
        if observed != evidence:
            wrong.append((case, "compare evidence", observed))
        result = current.check(baseline=baseline, max_null_percentage_increase={"x": 0})
        if result.verdict != verdict:
            wrong.append((case, "check verdict", result.verdict))
        path = tmp_path / f"baseline_{case}.json"
        path.write_text(json.dumps(summary), encoding="utf-8")
        cli = subprocess.run(
            [
                sys.executable,
                "-m",
                "dataprof.check",
                str(source),
                "--baseline",
                str(path),
                "--max-null-increase",
                "x=0",
                "--json",
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=60,
            check=False,
        )
        if cli.returncode != _EXIT_CODES[verdict]:
            wrong.append((case, "cli exit", cli.returncode, cli.stderr[-300:]))
    assert not wrong, wrong


def test_a_malformed_sampling_applied_is_still_refused() -> None:
    summary = _summary(None)
    summary["execution"]["sampling_applied"] = "false"
    with pytest.raises(ValueError, match="'execution.sampling_applied' must be a boolean"):
        dp.ProfileReport.from_dict(summary)
