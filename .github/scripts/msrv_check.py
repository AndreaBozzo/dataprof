"""Compile the advertised MSRV feature graphs on the MSRV toolchain (issue #674).

`rust-version` in the root `Cargo.toml` is the single source of truth for the
minimum supported Rust version. Before this gate existed nothing compiled
against it: ordinary CI installs the pinned 1.98 toolchain, and the one 1.96 job
in `release.yml` ran `cargo metadata --no-deps`, which parses manifests without
touching a line of source. A newer standard-library API, language feature or
dependency MSRV could pass every gate and still break a downstream build on the
promised minimum compiler.

What this script asserts:

1. Every workspace package declares the same `rust-version` (nothing drifts off
   the shared value inherited from `[workspace.package]`).
2. The *active* toolchain is that version. Running the gate on a newer compiler
   proves nothing, so a workflow pin that drifts away from `rust-version` fails
   loudly here instead of passing quietly.
3. Each declared feature graph in `FEATURE_GRAPHS` compiles under
   `cargo check --locked --lib`.

Scope, and why:

- `--lib` only. The MSRV promise is that the published *libraries* build for a
  downstream consumer. Dev-dependencies (criterion, jsonschema, proptest),
  examples and benches are development tooling and build on the pinned
  toolchain, not this one.
- `--locked`. The gate checks the dependency set a release actually ships,
  the one in `Cargo.lock`. It is not a minimal-version check: a dependency
  raising its own MSRV surfaces here when the lockfile is updated, which is
  exactly when a dependency-update PR runs this job.

Run it locally against the MSRV toolchain (rustup honours `RUSTUP_TOOLCHAIN`):

    rustup toolchain install 1.96
    RUSTUP_TOOLCHAIN=1.96 python3 .github/scripts/msrv_check.py
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

# The feature graphs the MSRV promise covers. Every published surface a
# downstream crate can turn on is listed: leaving one out means the promise is
# unverified for whoever enables it. Keep this in step with the release-surface
# list in `.github/workflows/release.yml`.
FEATURE_GRAPHS: list[tuple[str, list[str]]] = [
    ("facade, no default features", ["-p", "dataprof", "--no-default-features"]),
    ("facade, default features (parquet)", ["-p", "dataprof"]),
    (
        "facade, async-streaming",
        ["-p", "dataprof", "--no-default-features", "--features", "async-streaming"],
    ),
    ("facade, parquet-async", ["-p", "dataprof", "--features", "parquet-async"]),
    # Covers `database` and the postgres/mysql/sqlite connectors.
    ("facade, all features", ["-p", "dataprof", "--all-features"]),
    # docs/python/README.md tells users a source build needs Rust 1.96 or later,
    # so the extension crate is part of the promise, not just the pure-Rust
    # facade. Release wheels are built on the pinned toolchain; an sdist build
    # on a user's machine is what this arm stands for.
    ("python extension crate, all features", ["-p", "dataprof-python", "--all-features"]),
]

RUSTC_VERSION = re.compile(r"^rustc (\d+)\.(\d+)\.(\d+)")


def run(command: list[str], root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command, cwd=root, capture_output=True, text=True, encoding="utf-8", check=False
    )


def declared_msrv(root: Path) -> str:
    """The `rust-version` every workspace package agrees on."""
    result = run(["cargo", "metadata", "--no-deps", "--format-version", "1"], root)
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        raise SystemExit("error: cargo metadata failed")

    packages = json.loads(result.stdout)["packages"]
    missing = sorted(p["name"] for p in packages if not p.get("rust_version"))
    if missing:
        raise SystemExit(
            "error: workspace package(s) declare no rust-version: "
            + ", ".join(missing)
            + "\nAdd `rust-version.workspace = true` so the MSRV covers them."
        )

    declared = sorted({p["rust_version"] for p in packages})
    if len(declared) != 1:
        raise SystemExit(
            "error: workspace packages declare different rust-versions: " + ", ".join(declared)
        )
    return declared[0]


def active_toolchain_version() -> tuple[int, int, str]:
    result = subprocess.run(
        ["rustc", "--version"], capture_output=True, text=True, encoding="utf-8", check=False
    )
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        raise SystemExit("error: rustc --version failed")

    reported = result.stdout.strip()
    match = RUSTC_VERSION.match(reported)
    if match is None:
        raise SystemExit(f"error: cannot parse rustc version from {reported!r}")
    return int(match.group(1)), int(match.group(2)), reported


def assert_running_on_msrv(declared: str, reported: str, active: tuple[int, int]) -> None:
    parts = declared.split(".")
    wanted = (int(parts[0]), int(parts[1]))
    if active == wanted:
        return
    raise SystemExit(
        f"error: this gate must run on the declared MSRV {declared}, "
        f"but the active toolchain is {reported}.\n"
        "A newer compiler accepts code the MSRV rejects, so a mismatched run "
        "proves nothing.\n"
        "Bump the `dtolnay/rust-toolchain@` pin on the MSRV jobs and "
        "`rust-version` in Cargo.toml together, or run locally with "
        f"RUSTUP_TOOLCHAIN={declared}."
    )


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    declared = declared_msrv(root)
    major, minor, reported = active_toolchain_version()
    assert_running_on_msrv(declared, reported, (major, minor))

    print(f"MSRV gate: {reported} against declared rust-version {declared}\n")

    failures: list[str] = []
    for label, arguments in FEATURE_GRAPHS:
        command = ["cargo", "check", "--locked", "--lib", *arguments]
        print(f"==> {label}\n    {' '.join(command)}", flush=True)
        result = subprocess.run(command, cwd=root, check=False)
        if result.returncode != 0:
            failures.append(label)
            print(f"    FAILED on Rust {declared}", flush=True)

    if failures:
        print(
            f"\nerror: {len(failures)} feature graph(s) do not compile on the "
            f"declared MSRV {declared}:",
            file=sys.stderr,
        )
        for label in failures:
            print(f"  - {label}", file=sys.stderr)
        print(
            "\nEither keep the code within Rust "
            f"{declared}, or raise `rust-version` in Cargo.toml (and the pins "
            "in README.md, AGENTS.md, docs/CONTRIBUTING.md and the MSRV jobs) "
            "deliberately.",
            file=sys.stderr,
        )
        return 1

    print(f"\nMSRV gate passed: {len(FEATURE_GRAPHS)} feature graphs compile on {declared}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
