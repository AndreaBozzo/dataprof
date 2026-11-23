"""Run the README's "Gate a pipeline" snippet as written, once per verdict.

The snippet is the first gate code a user copies. An earlier version printed
only ``violations`` behind ``if not result.passed``, so an inconclusive result
printed nothing and exited 0. This test reads the block from the README itself,
so an edit that drops a verdict fails here.
"""

from __future__ import annotations

import contextlib
import io
import re
from pathlib import Path

import dataprof as dp
import pytest

README = Path(__file__).resolve().parents[2] / "README.md"

ROWS = "\n".join(f"{i},{i * 1.5:.1f}" for i in range(1, 41))


def _gate_block() -> str:
    section = README.read_text(encoding="utf-8").split("## Gate a pipeline", 1)[1]
    match = re.search(r"```python\n(.*?)```", section, re.DOTALL)
    assert match, "the README's Gate a pipeline section has no python block"
    block = match.group(1)
    assert "report.check(" in block, "the first python block in the section is not the gate"
    return block


def _run(report: dp.ProfileReport) -> tuple[str, str | None]:
    """The snippet's stdout, and its exit message when it exits."""
    out = io.StringIO()
    exit_message = None
    with contextlib.redirect_stdout(out):
        try:
            exec(_gate_block(), {"report": report})  # noqa: S102 - the README's own code
        except SystemExit as exc:
            exit_message = str(exc.code)
    return out.getvalue(), exit_message


@pytest.mark.parametrize(
    "header,rows,verdict,shown",
    [
        ("customer_id,amount", ROWS, "pass", None),
        ("customer_id,amount", ROWS + "\n,7.0", "fail", "failed: max_null_count [customer_id]"),
        ("client_id,amount", ROWS, "inconclusive", "not_evaluated: max_null_count [customer_id]"),
    ],
)
def test_readme_gate_shows_every_verdict(
    tmp_path: Path, header: str, rows: str, verdict: str, shown: str | None
):
    path = tmp_path / "orders.csv"
    path.write_text(f"{header}\n{rows}\n", encoding="utf-8")
    report = dp.profile(path)
    assert report.check(min_quality_score=0).verdict == "pass"  # the file itself profiles

    output, exit_message = _run(report)

    assert f": {verdict}:" in output, output
    if verdict == "pass":
        assert exit_message is None
    else:
        assert exit_message is not None and verdict in exit_message
        assert shown is not None and shown in output, output
