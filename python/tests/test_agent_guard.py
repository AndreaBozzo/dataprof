"""Security tests for the agent guard (issue #336).

Each test maps to a box on the hardening checklist that gates the MCP server.
Inputs on an agent surface come from a model, so the cases below are written as
an attacker would send them: traversal, symlink escape, absolute paths, oversize
files, argument overrides that try to lift their own ceiling.

Run after building the extension:
    uv run maturin develop
    uv run pytest python/tests/test_agent_guard.py -v
"""

from __future__ import annotations

import ast
import os
import sys
import time
from pathlib import Path
from typing import Any

import dataprof.agent
import pytest
from dataprof.agent import (
    AgentGuard,
    AgentSecurityError,
    AgentTimeoutError,
    PathNotAllowedError,
    ResourceLimitExceededError,
    SandboxPolicy,
)

CSV_BODY = "id,email,amount\n1,a@example.com,10\n2,b@example.com,20\n3,c@example.com,30\n"


@pytest.fixture
def sandbox(tmp_path: Path) -> Path:
    root = tmp_path / "sandbox"
    root.mkdir()
    (root / "data.csv").write_text(CSV_BODY)
    return root


@pytest.fixture
def outside(tmp_path: Path) -> Path:
    """A file the guard must never reach, as a sibling of the sandbox root."""
    secret = tmp_path / "outside"
    secret.mkdir()
    target = secret / "secret.csv"
    target.write_text("token\nsuper-secret-value\n")
    return target


@pytest.fixture
def guard(sandbox: Path) -> AgentGuard:
    return AgentGuard(SandboxPolicy(roots=sandbox))


# --- Path allow-list / sandbox root ---------------------------------------


def test_policy_requires_a_root() -> None:
    with pytest.raises(ValueError, match="at least one root"):
        SandboxPolicy(roots=[])


def test_policy_rejects_a_root_that_is_not_a_directory(sandbox: Path) -> None:
    with pytest.raises(ValueError, match="not an existing directory"):
        SandboxPolicy(roots=sandbox / "data.csv")


def test_resolves_a_file_inside_the_root(guard: AgentGuard, sandbox: Path) -> None:
    assert (
        guard.resolve_path("data.csv" if os.getcwd() == str(sandbox) else sandbox / "data.csv")
        == (sandbox / "data.csv").resolve()
    )


def test_profiles_a_sandboxed_file(guard: AgentGuard, sandbox: Path) -> None:
    report = guard.profile(sandbox / "data.csv")
    assert report.rows == 3


def test_multiple_roots_are_each_honored(tmp_path: Path) -> None:
    a, b = tmp_path / "a", tmp_path / "b"
    for d in (a, b):
        d.mkdir()
        (d / "data.csv").write_text(CSV_BODY)
    guard = AgentGuard(SandboxPolicy(roots=[a, b]))
    assert guard.resolve_path(a / "data.csv").parent == a.resolve()
    assert guard.resolve_path(b / "data.csv").parent == b.resolve()


# --- Malicious-path tests --------------------------------------------------


def test_rejects_parent_traversal(guard: AgentGuard, sandbox: Path, outside: Path) -> None:
    with pytest.raises(PathNotAllowedError):
        guard.resolve_path(sandbox / ".." / "outside" / "secret.csv")


def test_rejects_deep_traversal_to_system_paths(guard: AgentGuard, sandbox: Path) -> None:
    with pytest.raises(PathNotAllowedError):
        guard.resolve_path(sandbox / ".." / ".." / ".." / ".." / "etc" / "passwd")


def test_rejects_absolute_path_outside_root(guard: AgentGuard, outside: Path) -> None:
    with pytest.raises(PathNotAllowedError, match="outside the sandbox"):
        guard.resolve_path(outside)


def test_rejects_traversal_that_lands_back_inside(guard: AgentGuard, sandbox: Path) -> None:
    """`root/sub/../data.csv` resolves in-root and is allowed — resolution, not
    string matching on `..`, is what decides."""
    (sandbox / "sub").mkdir()
    assert (
        guard.resolve_path(sandbox / "sub" / ".." / "data.csv") == (sandbox / "data.csv").resolve()
    )


@pytest.mark.skipif(sys.platform == "win32", reason="symlink creation needs privilege on Windows")
def test_rejects_symlink_escaping_the_root(guard: AgentGuard, sandbox: Path, outside: Path) -> None:
    link = sandbox / "escape.csv"
    link.symlink_to(outside)
    with pytest.raises(PathNotAllowedError):
        guard.resolve_path(link)


@pytest.mark.skipif(sys.platform == "win32", reason="symlink creation needs privilege on Windows")
def test_rejects_symlink_via_a_linked_directory(
    guard: AgentGuard, sandbox: Path, outside: Path
) -> None:
    (sandbox / "linkdir").symlink_to(outside.parent, target_is_directory=True)
    with pytest.raises(PathNotAllowedError):
        guard.resolve_path(sandbox / "linkdir" / "secret.csv")


@pytest.mark.skipif(sys.platform == "win32", reason="symlink creation needs privilege on Windows")
def test_in_root_symlink_rejected_by_default_allowed_when_configured(sandbox: Path) -> None:
    link = sandbox / "alias.csv"
    link.symlink_to(sandbox / "data.csv")

    with pytest.raises(PathNotAllowedError, match="symlink"):
        AgentGuard(SandboxPolicy(roots=sandbox)).resolve_path(link)

    permissive = AgentGuard(SandboxPolicy(roots=sandbox, follow_symlinks=True))
    assert permissive.resolve_path(link) == (sandbox / "data.csv").resolve()


def test_rejects_a_missing_file(guard: AgentGuard, sandbox: Path) -> None:
    with pytest.raises(PathNotAllowedError, match="no such file"):
        guard.resolve_path(sandbox / "nope.csv")


def test_rejects_a_directory(guard: AgentGuard, sandbox: Path) -> None:
    (sandbox / "subdir").mkdir()
    with pytest.raises(PathNotAllowedError, match="not a regular file"):
        guard.resolve_path(sandbox / "subdir")


# --- No arbitrary shell / code execution -----------------------------------


def test_guard_module_contains_no_execution_or_socket_primitives() -> None:
    """The guard must not grow an eval/exec/subprocess path.

    Asserted structurally rather than by grepping strings, so a comment
    mentioning `subprocess` does not fail the build and an aliased import does
    not slip past.
    """
    source_file = dataprof.agent.__file__
    assert source_file is not None
    tree = ast.parse(Path(source_file).read_text())

    banned_modules = {"subprocess", "socket", "urllib", "requests", "httpx", "pty", "shutil"}
    banned_calls = {"eval", "exec", "compile", "__import__", "system", "popen", "spawn"}

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert alias.name.split(".")[0] not in banned_modules, f"guard imports {alias.name}"
        elif isinstance(node, ast.ImportFrom) and node.module:
            assert node.module.split(".")[0] not in banned_modules, (
                f"guard imports from {node.module}"
            )
        elif isinstance(node, ast.Call):
            name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
            assert name not in banned_calls, f"guard calls {name}()"


def test_rejects_non_path_sources(guard: AgentGuard) -> None:
    """In-memory sources have no path to check, so they cannot be sandboxed."""
    sources: list[Any] = [{"a": [1]}, [1, 2, 3], 42, None, b"id,name\n1,x\n"]
    for source in sources:
        with pytest.raises(PathNotAllowedError, match="expected a file path"):
            guard.resolve_path(source)


# --- Bounded resources -----------------------------------------------------


@pytest.mark.parametrize("field", ["max_file_bytes", "max_rows", "max_bytes", "timeout_seconds"])
def test_policy_rejects_non_positive_bounds(sandbox: Path, field: str) -> None:
    bound: dict[str, Any] = {field: 0}
    with pytest.raises(ValueError, match="must be positive"):
        SandboxPolicy(roots=sandbox, **bound)


def test_rejects_a_file_over_the_size_limit(sandbox: Path) -> None:
    guard = AgentGuard(SandboxPolicy(roots=sandbox, max_file_bytes=10))
    with pytest.raises(ResourceLimitExceededError, match="over the 10-byte limit"):
        guard.resolve_path(sandbox / "data.csv")


def test_oversize_file_never_reaches_the_engine(sandbox: Path) -> None:
    """The bound must be pre-flight — a guard that hands an oversize file to the
    engine has already paid the cost it was meant to prevent. The engine reads
    from Rust, so watching Python file opens would prove nothing; assert instead
    that the engine entry point is never called."""
    guard = AgentGuard(SandboxPolicy(roots=sandbox, max_file_bytes=10))
    calls: list[str] = []

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("dataprof.agent._profile", lambda *a, **k: calls.append("called"))
        with pytest.raises(ResourceLimitExceededError):
            guard.profile(sandbox / "data.csv")
    assert calls == []


def test_stop_condition_reflects_the_policy(sandbox: Path) -> None:
    guard = AgentGuard(SandboxPolicy(roots=sandbox, max_rows=5, max_bytes=99))
    assert guard.stop_condition() is not None


def test_caller_cannot_widen_its_own_ceiling(guard: AgentGuard, sandbox: Path) -> None:
    for reserved in ("max_rows", "stop_condition"):
        with pytest.raises(ResourceLimitExceededError, match="cannot be overridden"):
            guard.profile(sandbox / "data.csv", **{reserved: 10**9})


def test_analyze_structure_clamps_max_rows_to_the_policy(sandbox: Path) -> None:
    guard = AgentGuard(SandboxPolicy(roots=sandbox, max_rows=2))
    captured: dict[str, object] = {}

    def fake_structure(path: str, max_rows: int) -> str:
        captured["max_rows"] = max_rows
        return "ok"

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("dataprof.agent._analyze_structure", fake_structure)
        guard.analyze_structure(sandbox / "data.csv", max_rows=10**6)
    assert captured["max_rows"] == 2


def test_timeout_raises_and_is_reported_as_a_security_error(sandbox: Path) -> None:
    guard = AgentGuard(SandboxPolicy(roots=sandbox, timeout_seconds=0.05))
    with pytest.raises(AgentTimeoutError, match="exceeded the 0.05s limit"):
        guard.run(time.sleep, 5)


def test_timeout_does_not_fire_on_a_quick_call(guard: AgentGuard) -> None:
    assert guard.run(lambda: 7) == 7


def test_run_propagates_the_callee_error_unchanged(guard: AgentGuard) -> None:
    def boom() -> None:
        raise KeyError("inner")

    with pytest.raises(KeyError):
        guard.run(boom)


# --- No network access unless configured -----------------------------------


@pytest.mark.parametrize(
    "url",
    [
        "http://evil.test/data.csv",
        "https://evil.test/data.csv",
        "s3://bucket/key.csv",
        "gs://bucket/key.csv",
        "ftp://host/f.csv",
        "postgresql://user:pw@host/db",
        "mysql://user:pw@host/db",
        "HTTP://EVIL.TEST/data.csv",
    ],
)
def test_rejects_network_sources_by_default(guard: AgentGuard, url: str) -> None:
    with pytest.raises(PathNotAllowedError, match="network access is disabled"):
        guard.resolve_path(url)


def test_network_rejection_does_not_echo_the_url(guard: AgentGuard) -> None:
    """A refused URL may carry credentials; the message names the scheme only."""
    with pytest.raises(PathNotAllowedError) as excinfo:
        guard.resolve_path("postgresql://admin:hunter2@db.internal/prod")
    message = str(excinfo.value)
    assert "hunter2" not in message
    assert "db.internal" not in message
    assert "admin" not in message


def test_allow_network_still_sandboxes_the_path(sandbox: Path) -> None:
    """Enabling network access lifts the scheme check, not the sandbox: a URL
    is still not a file inside the root."""
    guard = AgentGuard(SandboxPolicy(roots=sandbox, allow_network=True))
    with pytest.raises(PathNotAllowedError):
        guard.resolve_path("https://evil.test/data.csv")


# --- Deterministic output --------------------------------------------------


def test_same_input_yields_identical_context(guard: AgentGuard, sandbox: Path) -> None:
    first = guard.llm_context(guard.profile(sandbox / "data.csv"))
    second = guard.llm_context(guard.profile(sandbox / "data.csv"))
    assert first == second


def test_column_order_is_stable_across_runs(guard: AgentGuard, sandbox: Path) -> None:
    runs = [list(guard.profile(sandbox / "data.csv").column_profiles) for _ in range(3)]
    assert runs[0] == runs[1] == runs[2] == ["id", "email", "amount"]


# --- Redaction enforced at the boundary ------------------------------------


def test_llm_context_refuses_samples_by_default(guard: AgentGuard, sandbox: Path) -> None:
    report = guard.profile(sandbox / "data.csv")
    with pytest.raises(AgentSecurityError, match="include_samples is disabled"):
        guard.llm_context(report, include_samples=True)


def test_llm_context_leaks_no_raw_cell_values(guard: AgentGuard, sandbox: Path) -> None:
    context = guard.llm_context(guard.profile(sandbox / "data.csv"))
    for raw in ("a@example.com", "b@example.com", "c@example.com"):
        assert raw not in context


def test_samples_require_an_explicit_policy_opt_in(sandbox: Path) -> None:
    guard = AgentGuard(SandboxPolicy(roots=sandbox, allow_samples=True))
    report = guard.profile(sandbox / "data.csv")
    assert isinstance(guard.llm_context(report, include_samples=True), str)


# --- Clear, non-leaky error messages ---------------------------------------


def test_rejection_message_does_not_leak_the_host_path(guard: AgentGuard, outside: Path) -> None:
    with pytest.raises(PathNotAllowedError) as excinfo:
        guard.resolve_path(outside)
    message = str(excinfo.value)
    assert str(outside) not in message
    assert str(outside.parent) not in message
    assert message.endswith("secret.csv")


def test_in_root_rejection_names_the_path_relative_to_the_root(
    guard: AgentGuard, sandbox: Path
) -> None:
    (sandbox / "subdir").mkdir()
    with pytest.raises(PathNotAllowedError) as excinfo:
        guard.resolve_path(sandbox / "subdir")
    message = str(excinfo.value)
    assert "subdir" in message
    assert str(sandbox) not in message


def test_sanitize_error_passes_guard_rejections_through(guard: AgentGuard) -> None:
    exc = PathNotAllowedError("path is outside the sandbox: x.csv")
    assert guard.sanitize_error(exc) == "path is outside the sandbox: x.csv"


def test_sanitize_error_withholds_foreign_error_detail(guard: AgentGuard, outside: Path) -> None:
    """An engine or OS error may carry an absolute path or a fragment of the
    file that failed to parse; neither may reach the model."""
    exc = OSError(f"failed parsing '{outside}': unexpected token 'super-secret-value'")
    sanitized = guard.sanitize_error(exc)
    assert str(outside) not in sanitized
    assert "super-secret-value" not in sanitized
    assert sanitized.startswith("OSError:")
