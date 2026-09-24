"""Type stubs for _findings."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, NoReturn

class Finding:
    """Something in the report that deserves attention."""

    code: str
    severity: str
    evidence: dict[str, Any]
    summary: str
    column: str | None

    def __init__(
        self,
        code: str,
        severity: str,
        evidence: dict[str, Any],
        summary: str,
        column: str | None = ...,
    ) -> None: ...
    def to_dict(self) -> dict[str, Any]: ...

class FindingsResult:
    """The findings a report supports, and the rules that could not look."""

    findings: tuple[Finding, ...]
    not_evaluated: tuple[dict[str, Any], ...]

    def __init__(
        self,
        findings: tuple[Finding, ...] = ...,
        not_evaluated: tuple[dict[str, Any], ...] = ...,
    ) -> None: ...
    def __iter__(self) -> Iterator[Finding]: ...
    def __bool__(self) -> NoReturn: ...
    def to_dict(self) -> dict[str, Any]: ...
    def to_json(self, indent: int = ...) -> str: ...

class _FindingPolicy:
    def __init__(
        self,
        *,
        null_heavy_percentage: float | None,
        mixed_types_percentage: float | None,
    ) -> None: ...
    # `ProfileReport` is deliberately not named here, for the reason _gate.pyi
    # gives: the stub and the runtime class are distinct symbols to a checker.
    def evaluate(self, report: Any) -> FindingsResult: ...
