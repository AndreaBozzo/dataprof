"""Workflow hardening that every file under .github must keep.

Third-party actions run with the job's token and secrets, so a tag or branch
that moves changes what runs; a checkout that persists its token hands it to
every build script that follows; and a value interpolated into a `run:` block
becomes shell code.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from test_wheel_feature_single_source import _job_body

ROOT = Path(__file__).resolve().parents[2]
FILES = sorted(p for p in (ROOT / ".github").rglob("*") if p.suffix in {".yml", ".yaml"})
PINNED = re.compile(r"^[\w.-]+/[\w.-]+(?:/[\w./-]+)?@[0-9a-f]{40}(?: # \S+)?$")


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _name(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def _checkout_steps(text: str) -> list[list[str]]:
    """The lines of each step that uses actions/checkout, its own keys included."""
    lines = text.splitlines()
    steps = []
    for i, line in enumerate(lines):
        found = re.match(r"^(\s*(?:- )?)uses: actions/checkout@", line)
        if not found:
            continue
        key_indent = len(found[1])
        step = [line]
        for following in lines[i + 1 :]:
            if not following.strip():
                continue
            if len(following) - len(following.lstrip()) < key_indent:
                break
            step.append(following)
        steps.append(step)
    return steps


@pytest.mark.parametrize("path", FILES, ids=_name)
def test_third_party_actions_are_pinned_to_a_commit(path):
    refs = re.findall(r"^\s*(?:- )?uses:\s*(.+?)\s*$", _read(path), re.M)
    loose = [ref for ref in refs if not ref.startswith("./") and not PINNED.match(ref)]
    assert loose == [], f"{_name(path)} uses actions by a movable ref: {loose}"


@pytest.mark.parametrize("path", FILES, ids=_name)
def test_checkouts_do_not_persist_the_token(path):
    for step in _checkout_steps(_read(path)):
        assert any(line.strip() == "persist-credentials: false" for line in step), (
            f"{_name(path)}: {step[0].strip()} keeps its token in .git/config"
        )


@pytest.mark.parametrize("path", FILES, ids=_name)
def test_no_job_gets_every_permission(path):
    """`write-all` grants what the narrower mappings below would never see."""
    assert not re.search(r"^\s*permissions:\s*write-all\b", _read(path), re.M), _name(path)


def test_the_checkout_scan_sees_every_checkout():
    """A step layout the scan misreads would pass the check above silently."""
    total = sum(_read(p).count("uses: actions/checkout@") for p in FILES)
    assert total == sum(len(_checkout_steps(_read(p))) for p in FILES)
    assert total > 0


def test_release_passes_the_tag_to_shell_through_env():
    release = _read(ROOT / ".github/workflows/release.yml")
    interpolated = [
        line.strip()
        for line in release.splitlines()
        if "outputs.tag }}" in line and not re.match(r"^\s*(?:tag|TAG): \$\{\{", line)
    ]
    assert interpolated == []


def test_only_the_sarif_upload_can_write_security_events():
    security = _read(ROOT / ".github/workflows/security.yml")
    assert "security-events: write" not in security.split("\njobs:\n", 1)[0]
    holders = [
        job
        for job in re.findall(r"^  ([\w-]+):\n", security.split("\njobs:\n", 1)[1], re.M)
        if "security-events: write" in _job_body(security, job)
    ]
    assert holders == ["trivy"]
    assert "upload-sarif@" in _job_body(security, "trivy")
