"""Run the contributor setup smoke path from a fresh checkout."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

STEPS = (
    ("Install Python development dependencies", ("uv", "sync")),
    ("Build the local Python extension", ("uv", "run", "maturin", "develop")),
    (
        "Run focused Python API tests",
        ("uv", "run", "pytest", "python/tests/test_python_api.py", "-q"),
    ),
    ("Run dataprof-core tests", ("cargo", "test", "-p", "dataprof-core")),
    ("Run dataprof-python tests", ("cargo", "test", "-p", "dataprof-python")),
)


def main() -> int:
    missing = [command for command in ("uv", "cargo") if shutil.which(command) is None]
    if missing:
        print(f"Missing required command(s): {', '.join(missing)}")
        return 2

    for description, command in STEPS:
        print(f"\n==> {description}", flush=True)
        try:
            subprocess.run(command, cwd=REPO_ROOT, check=True)
        except subprocess.CalledProcessError as error:
            print(f"\nContributor smoke failed during: {description}")
            return error.returncode or 1

    print("\nContributor setup smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
