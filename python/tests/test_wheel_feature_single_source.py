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


# --- the smoke test must keep pace with what we ship (#590) -----------------

WHEEL_SMOKE = REPO_ROOT / ".github/scripts/wheel_smoke.py"

# What each shipped feature is supposed to turn on in `dataprof.capabilities()`.
# Derived from `crates/dataprof-python/src/lib.rs`, where the flags are set from
# `cfg!`, and `python/dataprof/__init__.py`, which maps them to the public
# names. A feature that is only a prerequisite turns nothing on by itself.
SHIPPED_FEATURE_CAPABILITIES: dict[str, tuple[str, ...]] = {
    "python": (),
    "python-async": (),
    "async-streaming": ("async_streaming", "url_profiling"),
    "parquet-async": ("remote_parquet",),
}


def test_every_shipped_feature_has_a_declared_capability_meaning():
    """Adding a feature to the wheel must not be possible without saying what it
    turns on.

    This is the drift that would otherwise be silent: the shipped set grows, the
    wheel smoke test keeps passing because it never learned to check the new
    surface, and a wheel missing that surface ships green.
    """
    declared = set(_tool_maturin_features())
    mapped = set(SHIPPED_FEATURE_CAPABILITIES)
    assert declared == mapped, (
        f"[tool.maturin] features and SHIPPED_FEATURE_CAPABILITIES disagree: "
        f"only in pyproject={sorted(declared - mapped)}, "
        f"only here={sorted(mapped - declared)}. Update this map and add the "
        f"matching assertion to {WHEEL_SMOKE.name}."
    )


def test_wheel_smoke_asserts_every_shipped_capability():
    """Every capability the shipped features turn on is asserted against the
    installed wheel.

    `capabilities()` reads compile-time `cfg!` flags, so only a check running on
    the built artifact can catch a feature that failed to compile in.
    """
    smoke = WHEEL_SMOKE.read_text(encoding="utf-8")
    expected = sorted({name for names in SHIPPED_FEATURE_CAPABILITIES.values() for name in names})
    assert expected, "the shipped feature set turns on no capability at all"
    missing = [name for name in expected if f"caps.{name}" not in smoke]
    assert not missing, (
        f"{WHEEL_SMOKE.name} never asserts {missing} against the installed wheel; "
        "a wheel built without those features would pass the smoke job."
    )


def test_wheel_smoke_asserts_the_deliberate_absence_of_database():
    """The other half of the contract is a negative.

    Database connectors are deliberately excluded (#588), so the smoke test has
    to prove they are absent. Without this, quietly shipping them — or shipping
    a build that reports them while the decode path is still wrong — would pass.
    """
    smoke = WHEEL_SMOKE.read_text(encoding="utf-8")
    assert "not caps.database" in smoke, (
        f"{WHEEL_SMOKE.name} must assert that database support is absent from the wheel"
    )
    assert "database" not in _tool_maturin_features(), (
        "database is in the shipped feature set; update the smoke test's negative "
        "assertion and the source-build docs before landing that"
    )


CI_WORKFLOW = REPO_ROOT / ".github/workflows/ci.yml"

# The CI jobs that must exercise the configuration users actually receive, and
# so must never pass `--features`. Jobs deliberately building a different set
# (the async-URL matrix, the database legs) are not listed and are free to.
SHIPPED_SET_JOBS = ("python-tests", "examples-smoke")


def _job_body(workflow: str, job: str) -> str:
    """The lines of one top-level job, up to the next job key."""
    lines = workflow.splitlines()
    start = next(
        (i for i, line in enumerate(lines) if line == f"  {job}:"),
        None,
    )
    assert start is not None, f"{CI_WORKFLOW.name} has no job named {job!r}"
    body = []
    for line in lines[start + 1 :]:
        if re.match(r"^  \S", line):  # the next top-level job key
            break
        body.append(line)
    return "\n".join(body)


def test_shipped_set_jobs_build_without_a_feature_flag():
    """The jobs that run the suite must build exactly what ships.

    `maturin develop` with no `--features` falls back to `[tool.maturin]
    features`, so these jobs track the shipped set automatically. Passing the
    flag here would pin them to a set of their own, and the configuration users
    receive would go untested by any job — every one of them building either
    more or less than it (#590).
    """
    workflow = CI_WORKFLOW.read_text(encoding="utf-8")
    for job in SHIPPED_SET_JOBS:
        body = _job_body(workflow, job)
        develop = [line.strip() for line in body.splitlines() if "maturin develop" in line]
        assert develop, f"job {job!r} no longer builds the extension with maturin develop"
        for line in develop:
            assert "--features" not in line, (
                f"job {job!r} pins its own feature set ({line!r}); it must build the "
                "shipped set declared in [tool.maturin] features"
            )


# --- documented source-build commands must not downgrade the build (#592) ----

# Docs that tell a user how to build the extension from source.
SOURCE_BUILD_DOCS = (
    "docs/python/README.md",
    "docs/guides/database-connectors.md",
    "Makefile",
    "python/dataprof/__init__.py",
    "python/tests/test_column_order.py",
    "python/tests/test_database_api.py",
    "python/tests/test_database_option_parity.py",
)


def _documented_feature_lists(text: str) -> list[tuple[str, list[str]]]:
    r"""Every `--features` list belonging to a real build command or message.

    Shell continuations are joined first, then adjacent Python string literals,
    so a command or an error message split across source lines is still seen as
    one string. A bare `--features` fragment with no command name on the line is
    ignored: the README shows one on purpose, as the example of what not to do.
    """
    joined = re.sub(r"\\\s*\n\s*", " ", text)
    joined = re.sub(r'"\s*\n\s*"', "", joined)
    found = []
    for line in joined.splitlines():
        stripped = line.strip()
        if "maturin" not in stripped and "pip install" not in stripped:
            continue
        match = re.search(r"""--features[= ]["']?([a-z0-9,\-]+)""", stripped)
        if match:
            found.append((stripped, match.group(1).split(",")))
    return found


def test_documented_source_builds_keep_everything_the_wheel_ships():
    """A source build must be a superset of the wheel, never a downgrade.

    `--features` replaces `[tool.maturin] features` rather than extending it, so
    a documented command that lists only the extra features quietly produces a
    package *smaller* than `pip install dataprof` — database support, but no
    async, URL profiling, or remote Parquet. Nothing errors, which is what makes
    it worth a test (#592).
    """
    shipped = set(_tool_maturin_features())
    checked = 0
    for relative in SOURCE_BUILD_DOCS:
        path = REPO_ROOT / relative
        commands = _documented_feature_lists(path.read_text(encoding="utf-8"))
        # A listed file that matches nothing checks nothing, and would sit here
        # looking like coverage it does not provide.
        assert commands, (
            f"{relative} is listed as a source-build instruction but no build "
            "command was found in it; fix the matcher or drop the file"
        )
        for command, features in commands:
            missing = sorted(shipped - set(features))
            assert not missing, (
                f"{relative} documents a build that drops {missing} from the shipped "
                f"feature set: {command!r}. `--features` replaces the pyproject list, "
                "so every documented command must repeat it in full."
            )
            checked += 1
    assert checked, "no documented source-build commands found; has the wording changed?"


def test_feature_lists_survive_shell_continuations():
    r"""The parser must join a `\`-continued command however it is spaced.

    `--features` often sits on the continuation line, so a join that misses
    leaves the command invisible to the guards below: the flag line carries no
    command name and is filtered out, and the check silently passes on a
    command it never read. Trailing whitespace after the backslash is the case
    that matters, because nothing renders it.
    """
    backslash = chr(92)
    command = (
        "pip install dataprof --no-binary dataprof {bs}{pad}\n"
        '  --config-settings="build-args=--features python,parquet-async"'
    )

    for pad in ("", " ", "   "):
        text = command.format(bs=backslash, pad=pad)
        found = _documented_feature_lists(text)
        assert found, f"continuation with {len(pad)} trailing space(s) was not joined"
        assert found[0][1] == ["python", "parquet-async"]


def test_bare_feature_fragments_are_not_treated_as_commands():
    """The README shows an incomplete `--features` line on purpose, as the
    example of what not to do. Counting it as a documented command would make
    the docs guard fail on its own warning."""
    text = '# Wrong: drops async support\n--features "python,python-async,database,sqlite"'
    assert _documented_feature_lists(text) == []
