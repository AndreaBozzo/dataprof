"""The published interpreter set is declared, tested and verified on artifacts."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from zipfile import ZipFile

import pytest
from test_wheel_feature_single_source import _job_body

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "wheel_interpreters", ROOT / ".github/scripts/wheel_interpreters.py"
)
assert SPEC is not None and SPEC.loader is not None
contract = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(contract)


def wheel(
    directory: Path,
    interpreter: str,
    abi: str | None = None,
    *,
    platform: str = "manylinux_2_17_x86_64.manylinux2014_x86_64",
    archive_tag: str | None = None,
    requires: str = "<3.15,>=3.10",
) -> Path:
    abi = abi or interpreter
    path = directory / f"dataprof-0.12.0-{interpreter}-{abi}-{platform}.whl"
    tags = (
        [archive_tag] if archive_tag else [f"{interpreter}-{abi}-{p}" for p in platform.split(".")]
    )
    with ZipFile(path, "w") as archive:
        archive.writestr(
            "dataprof-0.12.0.dist-info/WHEEL",
            "Wheel-Version: 1.0\n" + "".join(f"Tag: {tag}\n" for tag in tags),
        )
        archive.writestr(
            "dataprof-0.12.0.dist-info/METADATA",
            f"Name: dataprof\nVersion: 0.12.0\nRequires-Python: {requires}\n",
        )
    return path


@pytest.fixture
def versions():
    return contract.load_versions()


def test_metadata_agrees_with_declared_interpreters(versions):
    contract.validate_metadata(versions)


@pytest.mark.parametrize("platform", ["win_amd64", "macosx_11_0_arm64", "manylinux_2_28_aarch64"])
def test_complete_wheel_set_passes(tmp_path, versions, platform):
    for version in versions:
        wheel(tmp_path, "cp" + version.replace(".", ""), platform=platform)
    contract.verify_wheels(tmp_path, versions)


def test_compressed_platform_tags_pass(tmp_path, versions):
    for version in versions:
        wheel(tmp_path, "cp" + version.replace(".", ""))
    contract.verify_wheels(tmp_path, versions)


@pytest.mark.parametrize("populated", [False, True])
def test_missing_interpreters_fail(tmp_path, versions, populated):
    if populated:
        wheel(tmp_path, "cp310")
    with pytest.raises(ValueError, match="missing interpreter wheels"):
        contract.verify_wheels(tmp_path, versions)


@pytest.mark.parametrize(
    ("interpreter", "abi"),
    [("cp315", "cp315"), ("cp314", "cp314t"), ("pp311", "pypy311_pp73"), ("cp310", "abi3")],
)
def test_unpublished_interpreters_and_abis_fail(tmp_path, versions, interpreter, abi):
    wheel(tmp_path, interpreter, abi)
    with pytest.raises(ValueError, match="unexpected interpreter/ABI"):
        contract.verify_wheels(tmp_path, versions)


def test_filename_cannot_hide_free_threaded_archive(tmp_path, versions):
    wheel(tmp_path, "cp314", archive_tag="cp314-cp314t-win_amd64", platform="win_amd64")
    with pytest.raises(ValueError, match="archive tags disagree"):
        contract.verify_wheels(tmp_path, versions)


def test_wheel_metadata_cannot_advertise_untested_python(tmp_path, versions):
    wheel(tmp_path, "cp310", requires=">=3.10")
    with pytest.raises(ValueError, match="Requires-Python differs"):
        contract.verify_wheels(tmp_path, versions)


def test_duplicate_interpreter_fails(tmp_path, versions):
    wheel(tmp_path, "cp310", platform="win_amd64")
    wheel(tmp_path, "cp310", platform="win32")
    with pytest.raises(ValueError, match="duplicate interpreter"):
        contract.verify_wheels(tmp_path, versions)


@pytest.mark.parametrize("declared", [[], ["3.14t"], ["3.15-dev"], ["3.10", "3.12"], ["3.10"] * 2])
def test_invalid_declaration_fails(tmp_path, declared):
    (tmp_path / ".github").mkdir()
    (tmp_path / ".github/python-interpreters.json").write_text(json.dumps({"cpython": declared}))
    with pytest.raises(ValueError):
        contract.load_versions(tmp_path)


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ('requires-python = ">=3.10,<3.15"', 'requires-python = ">=3.10"'),
        ('    "Programming Language :: Python :: 3.14",', ""),
        ("Implementation :: CPython", "Implementation :: PyPy"),
    ],
)
def test_metadata_drift_fails(tmp_path, versions, old, new):
    project = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert old in project
    (tmp_path / "pyproject.toml").write_text(project.replace(old, new), encoding="utf-8")
    with pytest.raises(ValueError):
        contract.validate_metadata(versions, tmp_path)


def test_workflows_consume_the_declaration_and_gate_uploads(versions):
    ci = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    release = (ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
    for workflow in (ci, release):
        producer = _job_body(workflow, "python-interpreters")
        assert 'wheel_interpreters.py --github-output "$GITHUB_OUTPUT"' in producer
    for job in ("python-tests", "python-wheel-smoke"):
        body = _job_body(ci, job)
        assert "needs: python-interpreters" in body
        assert "python: ${{ fromJSON(needs.python-interpreters.outputs.versions) }}" in body
        assert "python-version: ${{ matrix.python }}" in body
        assert "UV_PYTHON: ${{ matrix.python }}" in body
    build = _job_body(release, "python-wheels")
    assert "rust-release-checks, python-interpreters]" in build
    assert "python-version: ${{ needs.python-interpreters.outputs.setup-versions }}" in build
    assert "--interpreter ${{ needs.python-interpreters.outputs.interpreters }}" in build
    assert "--find-interpreter" not in build
    assert "PYO3_USE_ABI3_FORWARD_COMPATIBILITY" not in release
    verify = build.index("wheel_interpreters.py --wheels dist")
    assert build.index("args: --release") < verify < build.index("Rename wheels")
    assert verify < build.index("uses: actions/upload-artifact") < build.index("gh release upload")
    outputs = contract.github_outputs(versions)
    assert f"versions={json.dumps(versions)}\n" in outputs
    assert "interpreters=" + " ".join("python" + v for v in versions) + "\n" in outputs
    assert (
        "setup-versions<<PYTHON_VERSIONS\n" + "\n".join(versions) + "\nPYTHON_VERSIONS\n" in outputs
    )
