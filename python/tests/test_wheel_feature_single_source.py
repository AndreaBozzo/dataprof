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

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _release_workflow() -> str:
    return (REPO_ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")


def _tool_maturin_features() -> list[str]:
    """The `features` list from `[tool.maturin]`, read without a TOML parser.

    `tomllib` is 3.11+ and this project supports 3.10, where every CI leg runs;
    scanning the one section beats adding a backport dependency for one list.
    """
    text = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    section = re.search(r"^\[tool\.maturin\]\s*$(.*?)(?=^\[|\Z)", text, re.M | re.S)
    assert section, "pyproject.toml has no [tool.maturin] section"
    declaration = re.search(r"^features\s*=\s*\[(.*?)\]", section.group(1), re.M | re.S)
    assert declaration, "[tool.maturin] declares no features list"
    return re.findall(r'"([^"]+)"', declaration.group(1))


def test_pyproject_declares_a_nonempty_wheel_feature_set():
    assert _tool_maturin_features(), "the shipped feature set must be declared in pyproject.toml"


def _optional_dependency_extras() -> dict[str, str]:
    """Every `name = [...]` entry under `[project.optional-dependencies]`."""
    text = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    section = re.search(
        r"^\[project\.optional-dependencies\]\s*$(.*?)(?=^\[|\Z)", text, re.M | re.S
    )
    assert section, "pyproject.toml has no [project.optional-dependencies] section"
    return {
        name: body
        for name, body in re.findall(r"^(\S+)\s*=\s*\[(.*?)\]", section.group(1), re.M | re.S)
    }


def test_no_extra_resolves_to_nothing():
    """An extra may only exist if installing it installs something.

    `database = []` shipped for several releases as a placeholder for a
    compile-time feature (#588). `pip install dataprof[database]` succeeded and
    produced a package with no database support, which is a worse answer than
    no extra at all. Compile-time features belong in build instructions.
    """
    extras = _optional_dependency_extras()
    assert extras, "expected at least one optional-dependency extra"
    empty = sorted(name for name, body in extras.items() if not body.strip())
    assert not empty, (
        f"extras resolve to no requirements: {empty}. An extra that installs nothing "
        "promises an install path that does not exist; document the build instead."
    )


def _maturin_args_lines(workflow: str) -> list[str]:
    """The `args:` values of every maturin-action step (wheels + sdist).

    Scoped by step rather than by "has the word maturin appeared yet": the
    workflow also runs git-cliff with an ``args:`` of its own, and any action
    added later would be swept in by a file-position test.
    """
    args = []
    in_maturin_step = False
    for line in workflow.splitlines():
        stripped = line.strip()
        if stripped.startswith("- "):
            # A new step begins; its first key may sit on the dash line.
            in_maturin_step = False
            stripped = stripped[2:]
        if stripped.startswith("uses:"):
            in_maturin_step = "maturin-action" in stripped
        elif in_maturin_step and stripped.startswith("args:"):
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
