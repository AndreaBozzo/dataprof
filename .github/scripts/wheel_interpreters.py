"""Declare CI/build interpreters and verify each release leg's wheel set (#649).

Only standard, GIL-enabled CPython is supported. The version list lives in
.github/python-interpreters.json; preview, PyPy, abi3 and free-threaded wheels
must not accidentally become release artifacts through runner discovery.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from email.parser import Parser
from pathlib import Path
from zipfile import ZipFile

REPO_ROOT = Path(__file__).resolve().parents[2]


def load_versions(root: Path = REPO_ROOT) -> list[str]:
    versions = json.loads((root / ".github/python-interpreters.json").read_text())["cpython"]
    if not isinstance(versions, list) or not versions:
        raise ValueError("cpython must be a nonempty list of minor versions")
    if any(not isinstance(v, str) or not re.fullmatch(r"3\.\d+", v) for v in versions):
        raise ValueError("only standard CPython 3.x minor versions are supported")
    minors = [int(v.split(".")[1]) for v in versions]
    if minors != list(range(minors[0], minors[-1] + 1)):
        raise ValueError("CPython versions must be unique, sorted and contiguous")
    return versions


def requires_python(versions: list[str]) -> str:
    upper_minor = int(versions[-1].split(".")[1]) + 1
    return f">={versions[0]},<3.{upper_minor}"


def matches_requires_python(value: str | None, versions: list[str]) -> bool:
    # Maturin can reorder specifiers when writing METADATA.
    return value is not None and {s.strip() for s in value.split(",")} == set(
        requires_python(versions).split(",")
    )


def validate_metadata(versions: list[str], root: Path = REPO_ROOT) -> None:
    if sys.version_info >= (3, 11):
        import tomllib
    else:
        import tomli as tomllib

    project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    expected = requires_python(versions)
    if not matches_requires_python(project["requires-python"], versions):
        raise ValueError(f"requires-python must match the interpreter declaration: {expected}")
    prefix = "Programming Language :: Python :: "
    classifiers = project["classifiers"]
    declared = {c.removeprefix(prefix) for c in classifiers if re.fullmatch(prefix + r"3\.\d+", c)}
    if declared != set(versions):
        raise ValueError("Python version classifiers differ from the interpreter declaration")
    implementations = {c for c in classifiers if c.startswith(prefix + "Implementation :: ")}
    if implementations != {prefix + "Implementation :: CPython"}:
        raise ValueError("only CPython may be advertised as a supported implementation")


def github_outputs(versions: list[str]) -> str:
    return (
        f"versions={json.dumps(versions)}\n"
        f"interpreters={' '.join('python' + v for v in versions)}\n"
        "setup-versions<<PYTHON_VERSIONS\n" + "\n".join(versions) + "\nPYTHON_VERSIONS\n"
    )


def verify_wheels(directory: Path, versions: list[str]) -> None:
    """Require one wheel per declared interpreter in one platform/CPU build leg.

    Check the archive too: a renamed filename cannot hide a different ABI or
    incompatible Requires-Python metadata. Run before optimized-wheel renaming.
    """
    expected = {"cp" + v.replace(".", "") for v in versions}
    seen: set[str] = set()
    for wheel in sorted(directory.glob("*.whl")):
        parts = wheel.stem.split("-")
        if len(parts) not in (5, 6) or parts[0] != "dataprof":
            raise ValueError(f"unexpected wheel filename: {wheel.name}")
        interpreter, abi, platform = parts[-3:]
        if interpreter not in expected or abi != interpreter:
            raise ValueError(f"unexpected interpreter/ABI: {wheel.name}")
        if interpreter in seen:
            raise ValueError(f"duplicate interpreter wheel: {wheel.name}")
        with ZipFile(wheel) as archive:
            wheel_metadata = [n for n in archive.namelist() if n.endswith(".dist-info/WHEEL")]
            package_metadata = [n for n in archive.namelist() if n.endswith(".dist-info/METADATA")]
            if len(wheel_metadata) != 1 or len(package_metadata) != 1:
                raise ValueError(f"expected one WHEEL and METADATA document: {wheel.name}")
            wheel_info = Parser().parsestr(archive.read(wheel_metadata[0]).decode("utf-8"))
            package_info = Parser().parsestr(archive.read(package_metadata[0]).decode("utf-8"))
        expected_tags = {f"{interpreter}-{abi}-{p}" for p in platform.split(".")}
        if set(wheel_info.get_all("Tag", [])) != expected_tags:
            raise ValueError(f"archive tags disagree with filename: {wheel.name}")
        if not matches_requires_python(package_info["Requires-Python"], versions):
            raise ValueError(f"wheel Requires-Python differs from declaration: {wheel.name}")
        seen.add(interpreter)
    if seen != expected:
        raise ValueError(f"missing interpreter wheels in {directory}: {sorted(expected - seen)}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--github-output", type=Path)
    parser.add_argument("--wheels", type=Path, help="verify one platform/CPU build directory")
    args = parser.parse_args()
    try:
        versions = load_versions()
        validate_metadata(versions)
        if args.github_output:
            with args.github_output.open("a", encoding="utf-8") as output:
                output.write(github_outputs(versions))
        if args.wheels:
            verify_wheels(args.wheels, versions)
    except (ValueError, OSError) as error:
        print(f"Wheel interpreter contract failed: {error}", file=sys.stderr)
        return 1
    print(f"Wheel interpreter contract passed: CPython {', '.join(versions)} (GIL enabled)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
