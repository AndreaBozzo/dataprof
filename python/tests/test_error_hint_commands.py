"""The commands error hints suggest run as pasted, for any path (#1164).

The hints for compressed, zip and non-UTF-8 files used to put the path in POSIX
single quotes, so a path with an apostrophe ended the quote early, cmd.exe split
a path with a space, and the tools named are not on Windows. Each case writes a
real file under ``hint dir/o'brien.*``, takes the command from the error, runs
it through every shell the host has (sh; cmd.exe and PowerShell), and checks
what it produced. Following a hint must not replace a file the user already
had either.

``tests/error_hint_commands.rs`` is the Rust twin.
"""

from __future__ import annotations

import gzip
import io
import re
import shutil
import subprocess
import sys
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pytest

try:
    import dataprof as dp
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

CSV = b"id,city\n1,Rome\n2,Milan\n"
LATIN_TEXT = "id,city\n1,Zürich\n2,Málaga\n"
WINDOWS = sys.platform == "win32"


def _zstd(payload: bytes) -> bytes | None:
    try:
        from compression import zstd  # ty: ignore[unresolved-import]
    except ImportError:
        return None
    return zstd.compress(payload)


def _zip(payload: bytes) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("data.csv", payload)
    return buffer.getvalue()


@dataclass(frozen=True)
class Case:
    name: str
    file: str
    """File name under ``hint dir``."""
    payload: bytes | None
    output: str
    """What the command writes, relative to the working directory."""
    expected: bytes
    """The bytes it must write there."""
    keeps: str
    """A file the user may already have where the command writes, which it must
    leave alone."""
    needs: str | None = None
    """The program the command needs, when the host may not have it."""


CASES = [
    Case(
        "gzip",
        "o'brien.csv.gz",
        gzip.compress(CSV),
        "hint dir/o'brien.csv",
        CSV,
        "hint dir/o'brien.csv",
    ),
    # Windows ships no zstd decoder, and the hint says so.
    Case(
        "zstd",
        "o'brien.csv.zst",
        _zstd(CSV),
        "hint dir/o'brien.csv",
        CSV,
        "hint dir/o'brien.csv",
        "zstd",
    ),
    # Windows extracts into a directory of its own.
    Case(
        "zip",
        "o'brien.zip",
        _zip(CSV),
        "hint dir/o'brien.zip.contents/data.csv" if WINDOWS else "data.csv",
        CSV,
        "data.csv",
    ),
    Case(
        "latin-1",
        "o'brien.csv",
        LATIN_TEXT.encode("latin-1"),
        "hint dir/o'brien.csv.utf8.csv",
        LATIN_TEXT.encode(),
        "hint dir/o'brien.csv.utf8.csv",
    ),
]


def _sh(command: str, cwd: Path) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(["sh", "-c", command], cwd=cwd, capture_output=True)


def _cmd(command: str, cwd: Path) -> subprocess.CompletedProcess[bytes]:
    # `/s` strips the outer quotes and leaves the command as typed.
    return subprocess.run(f'cmd /d /s /c "{command}"', cwd=cwd, capture_output=True)


def _powershell(command: str, cwd: Path) -> subprocess.CompletedProcess[bytes]:
    script = cwd / "hint.ps1"
    script.write_text(f"{command}\nexit $LASTEXITCODE\n", encoding="utf-8-sig")
    try:
        return subprocess.run(
            ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(script)],
            cwd=cwd,
            capture_output=True,
        )
    finally:
        script.unlink()


Runner = Callable[[str, Path], subprocess.CompletedProcess[bytes]]

SHELLS: list[tuple[str, Runner]] = (
    [("cmd", _cmd), ("powershell", _powershell)] if WINDOWS else [("sh", _sh)]
)


def _available(case: Case) -> bool:
    return case.payload is not None and (case.needs is None or shutil.which(case.needs) is not None)


def _suggested_command(case: Case, tmp: Path, file: str | None = None) -> str:
    """Write ``case``'s payload to ``tmp/hint dir/file`` (its own file name by
    default) and return the command its error suggests."""
    assert case.payload is not None
    directory = tmp / "hint dir"
    directory.mkdir(parents=True)
    path = directory / (file or case.file)
    path.write_bytes(case.payload)

    with pytest.raises(ValueError) as excinfo:
        dp.profile(path)
    message = str(excinfo.value)
    found = re.search(r"`([^`]+)`", message)
    assert found is not None, f"no command in the message: {message}"
    return found.group(1)


def _misrun(case: Case, tmp: Path, shell: str, run: Runner) -> str | None:
    command = _suggested_command(case, tmp)
    result = run(command, tmp)
    if result.returncode != 0:
        return f"{shell} `{command}` failed: {result.stdout!r} {result.stderr!r}"
    produced = tmp / case.output
    written = produced.read_bytes() if produced.exists() else None
    if written != case.expected:
        return f"{shell} `{command}` wrote {written!r}"
    try:
        report = dp.profile(produced)
    except Exception as err:  # noqa: BLE001 - any failure is the finding
        return f"{shell} `{command}`: the result fails: {err}"
    if report.rows != 2:
        return f"{shell} `{command}`: the result has {report.rows} rows"
    return None


def test_every_suggested_command_runs_on_a_path_with_a_space_and_an_apostrophe(
    tmp_path: Path,
) -> None:
    wrong = []
    for case in filter(_available, CASES):
        for shell, run in SHELLS:
            problem = _misrun(case, tmp_path / f"{case.name}-{shell}", shell, run)
            if problem is not None:
                wrong.append(f"{case.name} / {shell}: {problem}")
    assert not wrong, "\n".join(wrong)


def test_no_suggested_command_replaces_a_file_already_there(tmp_path: Path) -> None:
    """A ``data.csv`` beside ``data.csv.gz``, or one where the archive extracts,
    survives following the hint: refusing is fine, replacing is not."""
    kept_bytes = b"kept,by,the,user\n"
    wrong = []
    for case in filter(_available, CASES):
        for shell, run in SHELLS:
            tmp = tmp_path / f"{case.name}-{shell}"
            command = _suggested_command(case, tmp)
            kept = tmp / case.keeps
            kept.write_bytes(kept_bytes)
            run(command, tmp)
            if kept.read_bytes() != kept_bytes:
                wrong.append(f"{case.name} / {shell}: `{command}` replaced {case.keeps}")
    assert not wrong, "\n".join(wrong)


def test_a_file_name_holding_a_command_runs_nothing(tmp_path: Path) -> None:
    """A file name is data, never code. Inside double quotes PowerShell runs a
    ``$(...)`` subexpression, so a name holding one must not reach a Windows
    command, and POSIX single quotes must keep ``$(...)`` literal."""
    prefix = "$(New-Item pwned) " if WINDOWS else "$(touch pwned) "
    wrong = []
    for case in filter(_available, CASES):
        for shell, run in SHELLS:
            tmp = tmp_path / f"{case.name}-{shell}"
            command = _suggested_command(case, tmp, prefix + case.file)
            run(command, tmp)
            if (tmp / "pwned").exists() or (tmp / "hint dir" / "pwned").exists():
                wrong.append(f"{case.name} / {shell}: `{command}` ran the name")
    assert not wrong, "\n".join(wrong)
