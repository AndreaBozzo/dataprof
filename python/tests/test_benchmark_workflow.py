"""Failure artifacts must remain downloadable without becoming published evidence."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = (ROOT / ".github/workflows/benchmarks.yml").read_text(encoding="utf-8")


@pytest.mark.parametrize(
    "failed_stage", [None, "dependencies", "preflight", "build", "rust", "comparison", "boundaries"]
)
def test_workflow_records_partial_outcomes_even_after_failure(tmp_path, failed_stage):
    """Execute the status writer to verify failures retain each stage's actual outcome."""
    # Execute the actual inline status writer without GitHub Actions or a Rust build.
    section = WORKFLOW.split("- name: Record run status\n", 1)[1].split(
        "- name: Upload results", 1
    )[0]
    assert "if: always()" in section
    script = textwrap.dedent(section.split("python3 - <<'PY'\n", 1)[1].rsplit("          PY", 1)[0])
    stages = ["dependencies", "preflight", "build", "rust", "comparison", "boundaries"]
    failed_index = stages.index(failed_stage) if failed_stage else len(stages)
    outcomes = {
        name: "success" if i < failed_index else "failure" if i == failed_index else "skipped"
        for i, name in enumerate(stages)
    }
    summary = tmp_path / "summary.md"
    subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env={
            **os.environ,
            "BENCHMARK_STEPS": json.dumps(
                {name: {"outcome": value} for name, value in outcomes.items()}
            ),
            "GITHUB_STEP_SUMMARY": str(summary),
        },
        check=True,
        timeout=10,
    )
    status = json.loads((tmp_path / "benchmark-results/run-status.json").read_text())
    assert status["status"] == ("incomplete" if failed_stage else "complete")
    assert status["stages"] == outcomes
    assert status["status"] in summary.read_text()


def test_workflow_uploads_both_suites_on_failure_and_publishes_only_successes():
    """Workflow failures retain diagnostics without exposing incomplete runs through Pages."""
    upload = WORKFLOW.split("- name: Upload results\n", 1)[1].split("- name: Summary", 1)[0]
    assert "if: always()" in upload
    assert "benchmark-results/" in upload
    assert "target/criterion/" in upload
    assert "continue-on-error:" not in WORKFLOW
    assert WORKFLOW.index("- name: Preflight comparison imports") < WORKFLOW.index(
        "- name: Build benchmarks"
    )
    for name in ("Run benchmarks", "Run tool comparison", "Run Python boundary experiment"):
        step = WORKFLOW.split(f"- name: {name}\n", 1)[1].split("- name:", 1)[0]
        assert "shell: bash" in step
        assert "set -euo pipefail" in step  # tee must not mask a failing runner.
    pages = (ROOT / ".github/workflows/deploy-pages.yml").read_text(encoding="utf-8")
    assert "github.event.workflow_run.conclusion == 'success'" in pages
    assert "--status success" in pages
    assert "comparison/progress.json" in pages
    assert "benchmark-results/run-status.json" in pages
