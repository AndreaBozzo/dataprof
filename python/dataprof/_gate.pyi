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
    bounds: dict[str, Any] | None
    baseline: dict[str, Any] | None

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
        bounds: dict[str, Any] | None = ...,
        baseline: dict[str, Any] | None = ...,
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
    baseline: dict[str, Any] | None

    def __init__(
        self,
        verdict: str,
        scope: str,
        evidence: dict[str, Any],
        checks: tuple[QualityCheck, ...] = ...,
        baseline: dict[str, Any] | None = ...,
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

def _real_number(value: Any) -> float: ...

class _PolicyFieldError(ValueError):
    key: str

    def __init__(self, key: str, message: str) -> None: ...

class _Policy:
    def __init__(
        self,
        *,
        min_rows: int | None,
        min_quality_score: float | None,
        min_dimension_scores: Mapping[str, float] | None,
        max_null_percentage: Mapping[str, float] | float | None,
        max_duplicate_rows: int | None,
        require_metrics: Sequence[str] | None,
        scope: str,
        baseline: Any = ...,
        max_quality_score_drop: float | None = ...,
        max_dimension_score_drops: Mapping[str, float] | None = ...,
        max_null_percentage_increase: Mapping[str, float] | float | None = ...,
        require_columns: Sequence[str] | None = ...,
        forbid_removed_columns: bool = ...,
        forbid_added_columns: bool = ...,
    ) -> None: ...
    # Takes the report's runtime object, or `None` for a report restored
    # from a flat summary, which the gate refuses.
    def evaluate(self, native: Any) -> QualityGateResult: ...
