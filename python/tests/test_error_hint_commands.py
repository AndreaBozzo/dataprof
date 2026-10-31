"""The commands error hints suggest run as pasted, for any path (#1164).

The hints for compressed, zip and non-UTF-8 files used to put the path in POSIX
single quotes, so a path with an apostrophe ended the quote early, cmd.exe split
a path with a space, and the tools named are not on Windows. Each case writes a
real file under ``hint dir/o'brien.*``, takes the command from the error, runs
it through every shell the host has (sh; cmd.exe and PowerShell), and profiles
what it produced.

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


# (name, file under "hint dir", bytes, what the command writes relative to the
# working directory, the program the command needs when the host may lack it)
CASES: list[tuple[str, str, bytes | None, str, str | None]] = [
    ("gzip", "o'brien.csv.gz", gzip.compress(CSV), "hint dir/o'brien.csv", None),
    # Windows ships no zstd decoder, and the hint says so.
    ("zstd", "o'brien.csv.zst", _zstd(CSV), "hint dir/o'brien.csv", "zstd"),
    ("zip", "o'brien.zip", _zip(CSV), "data.csv", None),
    (
        "latin-1",
        "o'brien.csv",
        "id,city\n1,Zürich\n2,Málaga\n".encode("latin-1"),
        "hint dir/o'brien.csv.utf8.csv",
        None,
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
    [("cmd", _cmd), ("powershell", _powershell)] if sys.platform == "win32" else [("sh", _sh)]
)


def _misrun(
    tmp: Path, file: str, payload: bytes, output: str, shell: str, run: Runner
) -> str | None:
    directory = tmp / "hint dir"
    directory.mkdir()
    path = directory / file
    path.write_bytes(payload)

    with pytest.raises(ValueError) as excinfo:
        dp.profile(path)
    message = str(excinfo.value)
    found = re.search(r"`([^`]+)`", message)
    if found is None:
        return f"no command in the message: {message}"
    command = found.group(1)

    result = run(command, tmp)
    if result.returncode != 0:
        return f"{shell} `{command}` failed: {result.stdout!r} {result.stderr!r}"
    try:
        report = dp.profile(tmp / output)
    except Exception as err:  # noqa: BLE001 - any failure is the finding
        return f"{shell} `{command}`: the result fails: {err}"
    if report.rows != 2:
        return f"{shell} `{command}`: the result has {report.rows} rows"
    return None


def test_every_suggested_command_runs_on_a_path_with_a_space_and_an_apostrophe(
    tmp_path: Path,
) -> None:
    wrong = []
    skipped = []
    for name, file, payload, output, needs in CASES:
        if payload is None or (needs is not None and shutil.which(needs) is None):
            skipped.append(name)
            continue
        for shell, run in SHELLS:
            tmp = tmp_path / f"{name}-{shell}"
            tmp.mkdir()
            problem = _misrun(tmp, file, payload, output, shell, run)
            if problem is not None:
                wrong.append(f"{name} / {shell}: {problem}")
    assert not wrong, "\n".join(wrong)
    if skipped:
        print(f"skipped, not available here: {skipped}")
