"""The release build's maturin and Linux build images are pinned (#1076).

`PyO3/maturin-action` otherwise installs the newest maturin that
`[build-system] requires` allows and runs the Linux legs in images chosen by
floating tags, so an upstream release changed the wheels between two tags with
no change in this repository. The pins live once, in the `env:` block of
`.github/workflows/python-dist.yml`; every maturin step reads them, and the
`[build-system] requires` floor is the pinned maturin, so a source install never
builds with an older one.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = REPO_ROOT / ".github/workflows/python-dist.yml"

IMAGE_KEYS = ("MANYLINUX_X86_64_IMAGE", "MANYLINUX_AARCH64_IMAGE")


def _workflow() -> str:
    return WORKFLOW.read_text(encoding="utf-8")


def _workflow_env() -> dict[str, str]:
    """The top-level `env:` block, which every job inherits."""
    lines = _workflow().splitlines()
    start = lines.index("env:")
    env = {}
    for line in lines[start + 1 :]:
        if line and not line.startswith(" "):
            break
        match = re.match(r"^  ([A-Z0-9_]+): (.+)$", line)
        if match:
            env[match.group(1)] = match.group(2).strip()
    return env


def _maturin_steps() -> list[dict[str, str]]:
    """The `with:` inputs of every maturin-action step, keyed by input name."""
    steps: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    for line in _workflow().splitlines():
        stripped = line.strip()
        if stripped.startswith("- "):
            current = None
            stripped = stripped[2:]
        if stripped.startswith("uses:"):
            current = {} if "maturin-action" in stripped else None
            if current is not None:
                steps.append(current)
            continue
        if current is not None:
            match = re.match(r"^([a-z-]+): (.+)$", stripped)
            if match:
                current[match.group(1)] = match.group(2)
    return steps


def _pinned_maturin() -> str:
    version = _workflow_env().get("MATURIN_VERSION", "")
    assert re.fullmatch(r"\d+\.\d+\.\d+", version), (
        f"MATURIN_VERSION must be one exact release, got {version!r}"
    )
    return version


def test_every_maturin_step_installs_the_pinned_version():
    steps = _maturin_steps()
    assert len(steps) >= 2, "expected the wheel and the sdist maturin steps"
    _pinned_maturin()
    unpinned = [
        step.get("command", "build")
        for step in steps
        if step.get("maturin-version") != "${{ env.MATURIN_VERSION }}"
    ]
    assert not unpinned, (
        f"maturin steps {unpinned} do not read MATURIN_VERSION, so the action installs "
        "whatever [build-system] requires allows"
    )


def test_build_system_floor_is_the_pinned_maturin():
    pyproject = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    requires = [req for req in pyproject["build-system"]["requires"] if req.startswith("maturin")]
    assert len(requires) == 1, f"expected one maturin requirement, got {requires}"
    match = re.fullmatch(r"maturin>=(\d+\.\d+\.\d+),<(\d+)\.0", requires[0].replace(" ", ""))
    assert match, f"expected maturin>=X.Y.Z,<N.0, got {requires[0]!r}"
    floor, ceiling = match.group(1), int(match.group(2))
    pinned = _pinned_maturin()
    assert floor == pinned, (
        f"[build-system] requires starts at maturin {floor}, the release build pins {pinned}; "
        "raise the floor with the pin"
    )
    assert int(pinned.split(".")[0]) < ceiling, f"maturin {pinned} is outside {requires[0]!r}"


def test_benchmark_build_uses_the_pinned_maturin():
    """The benchmark project builds dataprof from the checkout with its own exact
    maturin constraint. A constraint below the build-system floor makes that build
    unresolvable, which is how the first version of the pin broke the benchmarks."""
    benches = tomllib.loads((REPO_ROOT / "benches/pyproject.toml").read_text(encoding="utf-8"))
    constraints = benches["tool"]["uv"]["build-constraint-dependencies"]
    maturin = [c.replace(" ", "") for c in constraints if c.startswith("maturin")]
    assert maturin == [f"maturin=={_pinned_maturin()}"], (
        f"benches/pyproject.toml constrains the build to {maturin}, the release build pins "
        f"maturin {_pinned_maturin()}; bump both and run `uv lock` in benches/"
    )


def test_linux_build_images_are_pinned_by_digest():
    env = _workflow_env()
    for key in IMAGE_KEYS:
        image = env.get(key, "")
        assert re.fullmatch(r"[a-z0-9./_-]+:[A-Za-z0-9._-]+@sha256:[0-9a-f]{64}", image), (
            f"{key} must name an image by tag and digest, got {image!r}"
        )


def test_wheel_step_runs_the_linux_legs_in_the_pinned_images():
    builds = [step for step in _maturin_steps() if step.get("command", "build") == "build"]
    assert len(builds) == 1, "expected one wheel build step"
    container = builds[0].get("container", "")
    for key in IMAGE_KEYS:
        assert f"env.{key}" in container, (
            f"the wheel step's container input does not use {key}, so the action picks a "
            f"floating image: {container!r}"
        )
