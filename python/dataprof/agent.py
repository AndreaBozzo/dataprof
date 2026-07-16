"""Security guard for agent-facing surfaces.

Tool inputs on an agent surface come from an LLM, not from a trusted user, and
dataprof reads real files — so a careless agent surface is a data-exfiltration
vector. This module is the enforcement point that sits between untrusted input
and the profiling engine: it resolves paths against a sandbox root, bounds the
work a single call can do, and keeps file contents and host paths out of error
messages.

It is deliberately transport-agnostic. The MCP server (#330) is one caller; a
bare tool wrapper or a notebook helper is another. Nothing here imports an MCP
SDK, and nothing here is allowed to execute code or open sockets.

    from dataprof.agent import AgentGuard, SandboxPolicy

    guard = AgentGuard(SandboxPolicy(roots=["/srv/data"]))
    report = guard.profile("customers.csv")   # resolved under /srv/data
    print(guard.llm_context(report))          # redacted by construction

Every rejection raises a subclass of :class:`AgentSecurityError`, whose message
is safe to hand back to a model verbatim.
"""

from __future__ import annotations

import os
import pathlib
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, TimeoutError as _FutureTimeout
from dataclasses import dataclass
from typing import Any, TypeVar, cast

from . import (
    ProfileReport,
    StopCondition,
    StructureReport,
    analyze_structure as _analyze_structure,
    profile as _profile,
)

__all__ = [
    "AgentGuard",
    "AgentSecurityError",
    "AgentTimeoutError",
    "PathNotAllowedError",
    "ResourceLimitExceededError",
    "SandboxPolicy",
]

_T = TypeVar("_T")

# Schemes an LLM is most likely to hand us when it wants remote data. A source
# naming any of these is refused unless the policy opts into network access;
# the check is on the string the model supplied, before anything opens it.
_NETWORK_SCHEMES = (
    "http://",
    "https://",
    "ftp://",
    "ftps://",
    "s3://",
    "gs://",
    "azure://",
    "abfs://",
    "hdfs://",
    "postgres://",
    "postgresql://",
    "mysql://",
    "mongodb://",
)


class AgentSecurityError(Exception):
    """Base class for every guard rejection.

    The message is written to be safe to return to a model: it never contains
    file contents, and never contains a host path outside the sandbox root.
    """


class PathNotAllowedError(AgentSecurityError):
    """A source resolved outside the sandbox, or to something not a real file."""


class ResourceLimitExceededError(AgentSecurityError):
    """A call would exceed a configured bound (file size, rows, bytes)."""


class AgentTimeoutError(AgentSecurityError):
    """A call outlived the policy's per-call deadline."""


def _coerce_roots(
    roots: Sequence[str | os.PathLike[str]] | str | os.PathLike[str],
) -> tuple[pathlib.Path, ...]:
    items: Sequence[str | os.PathLike[str]]
    if isinstance(roots, (str, os.PathLike)):
        # A lone root is the common case; wrap it rather than making every
        # caller type `[root]`. The cast discards a spurious "sequence that is
        # also path-like" intersection the narrowing leaves behind.
        items = [cast("str | os.PathLike[str]", roots)]
    else:
        items = roots
    resolved: list[pathlib.Path] = []
    for r in items:
        p = pathlib.Path(r).expanduser().resolve()
        if not p.is_dir():
            raise ValueError(f"sandbox root is not an existing directory: {p}")
        resolved.append(p)
    if not resolved:
        raise ValueError(
            "SandboxPolicy requires at least one root; an unrooted guard sandboxes nothing"
        )
    return tuple(resolved)


# init=False: the validating __init__ below is the point of this class, and
# leaving it implicit would rely on dataclass declining to overwrite an
# __init__ already present in the class body.
@dataclass(frozen=True, init=False)
class SandboxPolicy:
    """Limits applied to every call made through an :class:`AgentGuard`.

    The defaults are deliberately conservative: an agent surface should start
    from "small, local, quick" and be widened on purpose, not discover its
    limits by profiling a 40 GB file.

    Args:
        roots: Directories file access is confined to. Required — there is no
            default root, because a guard without one enforces nothing.
        max_file_bytes: Largest file the guard will open, checked before the
            engine reads anything.
        max_rows: Row ceiling handed to the engine as a stop condition.
        max_bytes: Byte ceiling handed to the engine as a stop condition.
        timeout_seconds: Per-call wall-clock deadline. See :meth:`AgentGuard.run`
            for what this does and does not guarantee.
        follow_symlinks: When false (the default), a symlink whose target
            escapes the sandbox is rejected rather than followed.
        allow_network: When false (the default), sources naming a network
            scheme are rejected without being opened.
        allow_samples: When false (the default), :meth:`AgentGuard.llm_context`
            refuses to include raw sample values.
    """

    roots: tuple[pathlib.Path, ...]
    max_file_bytes: int = 256 * 1024 * 1024
    max_rows: int = 1_000_000
    max_bytes: int = 256 * 1024 * 1024
    timeout_seconds: float = 30.0
    follow_symlinks: bool = False
    allow_network: bool = False
    allow_samples: bool = False

    def __init__(
        self,
        roots: Sequence[str | os.PathLike[str]] | str | os.PathLike[str],
        *,
        max_file_bytes: int = 256 * 1024 * 1024,
        max_rows: int = 1_000_000,
        max_bytes: int = 256 * 1024 * 1024,
        timeout_seconds: float = 30.0,
        follow_symlinks: bool = False,
        allow_network: bool = False,
        allow_samples: bool = False,
    ) -> None:
        for name, value in (
            ("max_file_bytes", max_file_bytes),
            ("max_rows", max_rows),
            ("max_bytes", max_bytes),
        ):
            if value <= 0:
                raise ValueError(f"{name} must be positive, got {value}")
        if timeout_seconds <= 0:
            raise ValueError(f"timeout_seconds must be positive, got {timeout_seconds}")

        object.__setattr__(self, "roots", _coerce_roots(roots))
        object.__setattr__(self, "max_file_bytes", max_file_bytes)
        object.__setattr__(self, "max_rows", max_rows)
        object.__setattr__(self, "max_bytes", max_bytes)
        object.__setattr__(self, "timeout_seconds", timeout_seconds)
        object.__setattr__(self, "follow_symlinks", follow_symlinks)
        object.__setattr__(self, "allow_network", allow_network)
        object.__setattr__(self, "allow_samples", allow_samples)


class AgentGuard:
    """Enforces a :class:`SandboxPolicy` around the profiling entry points.

    Construct one per agent session and route every model-supplied source
    through it. A guard holds nothing but its frozen policy, so it carries no
    per-call state and is safe to share across threads.
    """

    def __init__(self, policy: SandboxPolicy) -> None:
        self._policy = policy

    @property
    def policy(self) -> SandboxPolicy:
        return self._policy

    # -- path handling ----------------------------------------------------

    def resolve_path(self, source: str | os.PathLike[str]) -> pathlib.Path:
        """Resolve a model-supplied source to a real file inside the sandbox.

        Traversal, absolute paths outside the root, and escaping symlinks are
        all rejected here — after full resolution, so that `a/../../etc/passwd`
        and a symlink to `/etc/passwd` fail the same check rather than relying
        on spotting `..` in the string.

        Raises:
            PathNotAllowedError: The source escapes the sandbox, does not
                exist, or is not a regular file.
            ResourceLimitExceededError: The file is larger than
                ``policy.max_file_bytes``.
        """
        raw = self._require_pathlike(source)
        self._reject_network(raw)

        candidate = pathlib.Path(raw).expanduser()

        # Resolve symlinks and `..` fully before comparing against the roots.
        # Comparing an unresolved path would let `root/../../etc` pass.
        try:
            resolved = candidate.resolve(strict=True)
        except (OSError, RuntimeError):
            # RuntimeError covers a symlink loop. Report both as "not found":
            # distinguishing them tells the caller about the host filesystem.
            raise PathNotAllowedError(
                f"no such file inside the sandbox: {self._describe(candidate)}"
            ) from None

        root = self._containing_root(resolved)
        if root is None:
            raise PathNotAllowedError(f"path is outside the sandbox: {self._describe(candidate)}")

        if not self._policy.follow_symlinks and self._traverses_symlink(candidate, resolved):
            raise PathNotAllowedError(
                f"path is a symlink and symlinks are disabled: {self._describe(candidate)}"
            )

        if not resolved.is_file():
            raise PathNotAllowedError(f"not a regular file: {self._relative(resolved, root)}")

        size = resolved.stat().st_size
        if size > self._policy.max_file_bytes:
            raise ResourceLimitExceededError(
                f"file is {size} bytes, over the {self._policy.max_file_bytes}-byte limit: "
                f"{self._relative(resolved, root)}"
            )
        return resolved

    def _containing_root(self, resolved: pathlib.Path) -> pathlib.Path | None:
        for root in self._policy.roots:
            try:
                resolved.relative_to(root)
            except ValueError:
                continue
            return root
        return None

    def _traverses_symlink(self, candidate: pathlib.Path, resolved: pathlib.Path) -> bool:
        """True if reaching ``resolved`` went through a link.

        Checked even when the target lands inside the sandbox: a link that
        currently resolves in-root can be repointed out of it later, and the
        strict reading of the checklist is that symlinks are off by default.
        """
        if candidate.is_symlink():
            return True
        try:
            absolute = candidate if candidate.is_absolute() else pathlib.Path.cwd() / candidate
            return os.path.normpath(absolute) != str(resolved)
        except OSError:
            return True

    def _reject_network(self, raw: str) -> None:
        if self._policy.allow_network:
            return
        lowered = raw.lower()
        for scheme in _NETWORK_SCHEMES:
            if lowered.startswith(scheme):
                # Name the scheme, not the URL: the URL may carry credentials.
                raise PathNotAllowedError(
                    f"network access is disabled; refusing a {scheme.rstrip(':/')} source"
                )

    @staticmethod
    def _require_pathlike(source: Any) -> str:
        if isinstance(source, str):
            return source
        if isinstance(source, os.PathLike):
            fs = os.fspath(source)
            if isinstance(fs, str):
                return fs
        raise PathNotAllowedError(
            f"expected a file path, got {type(source).__name__}; "
            "in-memory sources bypass the sandbox and are not accepted here"
        )

    def _describe(self, candidate: pathlib.Path) -> str:
        """Render a rejected path without disclosing the host filesystem.

        A rejected path resolved outside the sandbox, so there is no in-root
        form to show and the resolved form is exactly what must not leak. Show
        only the final component the caller already knows it sent.
        """
        return candidate.name or "<empty path>"

    @staticmethod
    def _relative(resolved: pathlib.Path, root: pathlib.Path) -> str:
        return resolved.relative_to(root).as_posix()

    # -- bounded execution ------------------------------------------------

    def stop_condition(self) -> StopCondition:
        """The engine-level row and byte ceilings implied by the policy."""
        return StopCondition.max_rows(self._policy.max_rows) | StopCondition.max_bytes(
            self._policy.max_bytes
        )

    def run(self, fn: Callable[..., _T], *args: Any, **kwargs: Any) -> _T:
        """Run ``fn`` under the policy's per-call deadline.

        The deadline bounds what the caller waits for and what the agent sees.
        It is **not** preemptive cancellation: the native profiling call holds
        no cancellation token, and progress callbacks cannot carry one either
        (``crates/dataprof-python/src/progress.rs`` swallows callback errors by
        design, so raising from one would not stop the work). On timeout the
        worker thread runs to completion in the background and its result is
        discarded.

        The real defense against runaway work is therefore the pre-flight size
        check in :meth:`resolve_path` plus :meth:`stop_condition`, both of which
        cap the work before it starts. This deadline is the backstop.

        Raises:
            AgentTimeoutError: ``fn`` did not finish within the deadline.
        """
        # A dedicated executor per call keeps a timed-out straggler from
        # occupying a shared pool slot for the life of the guard.
        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="dataprof-agent")
        try:
            future = executor.submit(fn, *args, **kwargs)
            try:
                return future.result(timeout=self._policy.timeout_seconds)
            except _FutureTimeout:
                future.cancel()
                raise AgentTimeoutError(
                    f"call exceeded the {self._policy.timeout_seconds:g}s limit and was abandoned"
                ) from None
        finally:
            # Do not block on a straggler; the thread is a daemon of the pool
            # and the interpreter joins it at exit.
            executor.shutdown(wait=False)

    # -- guarded entry points ---------------------------------------------

    def profile(self, source: str | os.PathLike[str], **kwargs: Any) -> ProfileReport:
        """Profile a sandboxed file under the policy's bounds.

        The row and byte ceilings are set from the policy and cannot be widened
        by the caller — an LLM-supplied argument must not be able to raise its
        own ceiling. They travel as a single ``stop_condition``, which the
        underlying ``profile()`` refuses to accept alongside ``max_rows``.

        Raises:
            AgentSecurityError: The source or arguments violate the policy.
        """
        path = self.resolve_path(source)
        for reserved in ("stop_condition", "max_rows", "source"):
            if reserved in kwargs:
                raise ResourceLimitExceededError(
                    f"{reserved!r} is fixed by the sandbox policy and cannot be overridden per call"
                )
        return self.run(_profile, str(path), stop_condition=self.stop_condition(), **kwargs)

    def analyze_structure(
        self, source: str | os.PathLike[str], max_rows: int | None = None
    ) -> StructureReport:
        """Cheap structural first look at a sandboxed file.

        ``max_rows`` is clamped to the policy ceiling rather than rejected, so
        a model asking for more simply gets the ceiling.
        """
        path = self.resolve_path(source)
        bounded = (
            self._policy.max_rows if max_rows is None else min(max_rows, self._policy.max_rows)
        )
        return self.run(_analyze_structure, str(path), bounded)

    def llm_context(
        self, report: ProfileReport, max_tokens: int = 1000, include_samples: bool = False
    ) -> str:
        """Render ``report`` as bounded, redacted model context.

        Raises:
            AgentSecurityError: ``include_samples`` was requested but the
                policy does not allow raw values.
        """
        if include_samples and not self._policy.allow_samples:
            raise AgentSecurityError(
                "include_samples is disabled by the sandbox policy; "
                "raw values must not cross the agent boundary"
            )
        return report.to_llm_context(max_tokens=max_tokens, include_samples=include_samples)

    def sanitize_error(self, exc: BaseException) -> str:
        """Render any exception as a message safe to hand back to a model.

        Guard rejections are already written to be safe and pass through. Every
        other exception is reduced to its type: an arbitrary error from the
        engine or the filesystem may carry an absolute path or a fragment of
        the file that failed to parse, and neither may reach the model.
        """
        if isinstance(exc, AgentSecurityError):
            return str(exc)
        return f"{type(exc).__name__}: request failed (details withheld at the agent boundary)"
