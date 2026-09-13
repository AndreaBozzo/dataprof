"""Type stubs for _gate."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Any

class QualityCheck:
    """One requirement and what the report said about it."""

    code: str
    expected: dict[str, Any]
    scope: str
    evidence: dict[str, Any]
    status: str
    message: str
    column: str | None
    dimension: str | None
    observed: float | int | None
    reason: dict[str, Any] | None

    def __init__(
        self,
        code: str,
        expected: dict[str, Any],
        scope: str,
        evidence: dict[str, Any],
        status: str,
        message: str,
        column: str | None = ...,
        dimension: str | None = ...,
        observed: float | int | None = ...,
        reason: dict[str, Any] | None = ...,
    ) -> None: ...
    @property
    def is_violation(self) -> bool: ...
    def to_dict(self) -> dict[str, Any]: ...

class QualityGateResult:
    """The structured result of evaluating a policy against a report."""

    verdict: str
    scope: str
    evidence: dict[str, Any]
    checks: tuple[QualityCheck, ...]

    def __init__(
        self,
        verdict: str,
        scope: str,
        evidence: dict[str, Any],
        checks: tuple[QualityCheck, ...] = ...,
    ) -> None: ...
    @property
    def passed(self) -> bool: ...
    @property
    def violations(self) -> list[QualityCheck]: ...
    @property
    def unevaluated(self) -> list[QualityCheck]: ...
    def __bool__(self) -> bool: ...
    def __iter__(self) -> Iterator[QualityCheck]: ...
    def to_dict(self) -> dict[str, Any]: ...
    def to_json(self, indent: int = ...) -> str: ...

class _Policy:
    def __init__(
        self,
        *,
        min_quality_score: float | None,
        min_dimension_scores: Mapping[str, float] | None,
        max_null_percentage: Mapping[str, float] | float | None,
        max_duplicate_rows: int | None,
        require_metrics: Sequence[str] | None,
        scope: str,
    ) -> None: ...
    # `ProfileReport` is deliberately not named here: the stub for it and the
    # runtime class are distinct symbols to a checker, so importing it would
    # reject `_report.py` passing its own `self`.
    def evaluate(self, report: Any) -> QualityGateResult: ...
