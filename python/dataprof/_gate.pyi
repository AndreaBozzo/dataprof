"""Type stubs for _gate."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any, Literal

_Verdict = Literal["pass", "fail", "inconclusive"]

class QualityCheck:
    """One requirement and what the report said about it."""

    code: str
    expected: dict[str, Any]
    scope: str
    evidence: dict[str, Any]
    status: Literal["passed", "failed", "not_evaluated", "not_applicable"]
    message: str
    column: str | None
    dimension: str | None
    observed: float | int | None
    reason: dict[str, Any] | None
    bounds: dict[str, Any] | None
    baseline: dict[str, Any] | None
    finding: str | None
    findings: tuple[dict[str, Any], ...]

    def __init__(
        self,
        code: str,
        expected: dict[str, Any],
        scope: str,
        evidence: dict[str, Any],
        status: Literal["passed", "failed", "not_evaluated", "not_applicable"],
        message: str,
        column: str | None = ...,
        dimension: str | None = ...,
        observed: float | int | None = ...,
        reason: dict[str, Any] | None = ...,
        bounds: dict[str, Any] | None = ...,
        baseline: dict[str, Any] | None = ...,
        finding: str | None = ...,
        findings: tuple[dict[str, Any], ...] = ...,
    ) -> None: ...
    @property
    def is_violation(self) -> bool: ...
    def to_dict(self) -> dict[str, Any]: ...

class QualityGateResult:
    """The structured result of evaluating a policy against a report."""

    verdict: _Verdict
    scope: str
    evidence: dict[str, Any]
    checks: tuple[QualityCheck, ...]
    baseline: dict[str, Any] | None
    generator: dict[str, Any] | None
    source_identity: dict[str, Any] | None
    baseline_missing: bool

    def __init__(
        self,
        verdict: _Verdict,
        scope: str,
        evidence: dict[str, Any],
        checks: tuple[QualityCheck, ...] = ...,
        baseline: dict[str, Any] | None = ...,
        generator: dict[str, Any] | None = ...,
        source_identity: dict[str, Any] | None = ...,
        baseline_missing: bool = ...,
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
    def __str__(self) -> str: ...

def combine_verdicts(results: Iterable[QualityGateResult]) -> _Verdict: ...
def _summary_lines(
    result: QualityGateResult, *, version: str, baseline_path: object = ...
) -> list[str]: ...
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
        max_null_count: Mapping[str, int] | int | None = ...,
        max_date_age: Mapping[str, int] | None = ...,
        max_date_gap: Mapping[str, int] | None = ...,
        unique_columns: Sequence[str] | None = ...,
        baseline: Any = ...,
        allow_missing_baseline: bool = ...,
        max_quality_score_drop: float | None = ...,
        max_dimension_score_drops: Mapping[str, float] | None = ...,
        max_null_percentage_increase: Mapping[str, float] | float | None = ...,
        max_null_count_increase: Mapping[str, int] | int | None = ...,
        max_distinct_drop: Mapping[str, float] | float | None = ...,
        max_row_count_drop: float | None = ...,
        max_row_count_growth: float | None = ...,
        require_columns: Sequence[str] | None = ...,
        forbid_removed_columns: bool = ...,
        forbid_added_columns: bool = ...,
        forbid_findings: Sequence[str] | None = ...,
        timeliness_clock: str = ...,
    ) -> None: ...
    # Takes the report's runtime object, or `None` for a report restored
    # from a flat summary, which the gate refuses.
    def evaluate(self, native: Any) -> QualityGateResult: ...
