"""Tests for the script bundled with the Claude skill.

``dp_context.py`` is executed by an agent, not imported, so it is tested the way
it is used: as a subprocess, asserting on stdout and the exit code. It is not
part of the dataprof package and never will be — see its module docstring — but
it ships in this repo, so it gets the same coverage as anything else that runs
in front of a user.

    uv run pytest python/tests/test_skill_script.py -v
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SKILL = REPO_ROOT / ".claude/skills/dataprof"
SCRIPT = SKILL / "scripts/dp_context.py"
FIXTURES = SKILL / "evals/fixtures"

# Values that live in customers_pii.csv. None may appear in any output.
SECRETS = (
    "alice@example.com",
    "bob@example.com",
    "+39 320 1234567",
    "IT60X0542811101000000123456",
)


def run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        timeout=120,
    )


def test_summary_reports_trust_signals() -> None:
    """A ragged read is reported as ragged, not as clean statistics."""
    result = run(str(FIXTURES / "ragged_orders.csv"), "--max-tokens", "400")

    assert result.returncode == 0, result.stderr
    assert "ragged_row_count: 2" in result.stdout
    assert "caveats" in result.stdout
    assert "future_dates_count" in result.stdout


def test_unassessed_dimensions_are_named_not_omitted() -> None:
    """A dimension that was not assessed says so rather than being dropped.

    An omitted row reads as a clean result, which is the absence-rule failure
    the skill exists to prevent.
    """
    result = run(str(FIXTURES / "customers_pii.csv"), "--max-tokens", "300")

    assert result.returncode == 0, result.stderr
    assert "not assessed" in result.stdout


@pytest.mark.parametrize(
    "args",
    [
        pytest.param(["--max-tokens", "800"], id="summary"),
        pytest.param(["--column", "email"], id="column"),
        pytest.param(["--column", "iban"], id="column-iban"),
        pytest.param(["--structure-only"], id="structure"),
    ],
)
def test_no_mode_emits_sensitive_values(args: list[str]) -> None:
    """No invocation prints a value from a column with a sensitive pattern.

    The script has no --include-samples escape hatch by design. If one is ever
    added, this test is the thing that should stop it.
    """
    result = run(str(FIXTURES / "customers_pii.csv"), *args)

    assert result.returncode == 0, result.stderr
    leaked = [s for s in SECRETS if s in result.stdout or s in result.stderr]
    assert not leaked, f"dp_context.py {' '.join(args)} leaked {leaked}"


def test_column_mode_reports_pattern_names() -> None:
    """Detecting the pattern is the point; withholding the values is the rule."""
    result = run(str(FIXTURES / "customers_pii.csv"), "--column", "email")

    assert result.returncode == 0, result.stderr
    assert '"Email"' in result.stdout


def test_structure_mode_scales_null_ratio_to_a_percentage() -> None:
    """null_ratio is 0..1; printing it with a % sign would be wrong by 100x.

    inventory_before.csv has 3 of 6 price values missing.
    """
    result = run(str(FIXTURES / "inventory_before.csv"), "--structure-only")

    assert result.returncode == 0, result.stderr
    assert "price: float nulls=50.0%" in result.stdout
    assert "rows: 6" in result.stdout  # not the RowCountEstimate repr
    assert "RowCountEstimate" not in result.stdout


def test_compare_mode_emits_deltas() -> None:
    result = run(
        str(FIXTURES / "inventory_before.csv"),
        "--compare",
        str(FIXTURES / "inventory_after.csv"),
    )

    assert result.returncode == 0, result.stderr
    assert '"quality_score"' in result.stdout
    assert '"completeness"' in result.stdout


def test_sandbox_root_resolves_relative_paths() -> None:
    """--root profiles inside the sandbox without a per-call limit override.

    Regression: forwarding max_rows=None tripped the guard's override check, so
    --root failed on every call that did not also pass --max-rows.
    """
    result = run("inventory_after.csv", "--root", str(FIXTURES), "--max-tokens", "150")

    assert result.returncode == 0, result.stderr
    assert "rows: 3" in result.stdout


def test_sandbox_rejects_traversal_without_a_traceback() -> None:
    result = run("../../../../etc/passwd", "--root", str(FIXTURES))

    assert result.returncode == 1
    assert "PathNotAllowedError" in result.stderr
    assert "Traceback" not in result.stderr


def test_invalid_sandbox_root_is_sanitized() -> None:
    """A bad --root fails cleanly instead of raising through argparse.

    Regression: SandboxPolicy validates the root, and building the guard sat
    outside the try block, so `--root /nonexistent` printed a traceback whose
    ValueError named the resolved absolute host path.
    """
    result = run("x.csv", "--root", "/nonexistent/sandbox/path")

    assert result.returncode == 1
    assert "Traceback" not in result.stderr
    assert "details withheld" in result.stderr
    assert "nonexistent" not in result.stderr


def test_engine_failures_do_not_echo_their_message() -> None:
    """A non-guard failure is reduced to its type at the agent boundary.

    Engine and filesystem errors can carry an absolute path or a fragment of the
    row that failed to parse. Profiling a directory raises one such error.
    """
    result = run("docs")

    assert result.returncode == 1
    assert "Traceback" not in result.stderr
    assert "details withheld" in result.stderr
    assert "engines failed" not in result.stderr


def test_guard_rejections_keep_their_safe_message() -> None:
    """Sanitizing must not flatten guard errors, which are written to be shown.

    Without this, the previous test could be satisfied by withholding
    everything, leaving an agent with no idea why a path was refused.
    """
    result = run("../../../../etc/passwd", "--root", str(FIXTURES))

    assert result.returncode == 1
    assert "PathNotAllowedError" in result.stderr
    assert "details withheld" not in result.stderr


def test_missing_file_is_reported_cleanly() -> None:
    result = run(str(FIXTURES / "does_not_exist.csv"))

    assert result.returncode == 1
    assert "no such file" in result.stderr
    assert "Traceback" not in result.stderr


def test_conflicting_modes_are_rejected() -> None:
    result = run(str(FIXTURES / "inventory_after.csv"), "--column", "sku", "--compare", "x.csv")

    assert result.returncode == 2
    assert "mutually exclusive" in result.stderr
