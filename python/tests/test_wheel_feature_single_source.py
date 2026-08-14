"""The shipped wheel feature set lives in exactly one place.

`pip install dataprof` gets a wheel built by `.github/workflows/release.yml`
while `pip install --no-binary dataprof` builds the sdist through the PEP 517
backend (maturin reading `pyproject.toml`). If the feature list is declared
in both places and they drift, the two install paths silently diverge in
capability. The rule: `[tool.maturin] features` in `pyproject.toml` is the
single declaration, and neither the wheel nor the sdist step in the release
workflow passes `--features` (maturin falls back to the pyproject config when
the CLI flag is absent).
"""
from __future__ import annotations

from pathlib import Path

import tomllib

REPO_ROOT = Path(__file__).resolve().parents[2]


def _release_workflow() -> str:
    return (REPO_ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")


def _tool_maturin_features() -> list[str]:
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    features = pyproject["tool"]["maturin"]["features"]
    assert isinstance(features, list), "expected a list of feature names"
    return features


def test_pyproject_declares_a_nonempty_wheel_feature_set():
    assert _tool_maturin_features(), "the shipped feature set must be declared in pyproject.toml"


def _maturin_args_lines(workflow: str) -> list[str]:
    """The `args:` values of every maturin-action step (wheels + sdist)."""
    args = []
    for line in workflow.splitlines():
        stripped = line.strip()
        if stripped.startswith("args: --") and "maturin" in workflow[: workflow.find(line)]:
            args.append(stripped)
    return args


def test_release_workflow_does_not_duplicate_the_feature_flag():
    """Neither the wheel nor the sdist maturin step may pass `--features`:
    maturin reads `[tool.maturin] features` from pyproject.toml when the CLI
    flag is absent, so a second declaration here is how the two install paths
    drift. (Cargo `cargo check/test --features` lines in the same file are CI
    matrix checks, not packaging, and are not governed by this rule.)"""
    args = _maturin_args_lines(_release_workflow())
    assert args, "expected at least one maturin build/sdist step in the release workflow"
    for line in args:
        assert "--features" not in line, (
            f"maturin step must not pass --features ({line!r}); the shipped set is "
            "declared once, in [tool.maturin] features in pyproject.toml"
        )
