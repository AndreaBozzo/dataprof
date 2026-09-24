"""Declarative quality gates over a :class:`~dataprof.ProfileReport`.

This mirrors ``dataprof_runtime::quality_gate`` in Rust. Both layers evaluate
the same requirements in the same order and produce the same document; the
shared fixture in ``tests/fixtures/quality_gate_parity.json`` asserts it from
both sides, so changing one implementation alone fails that layer's test.

Nothing here exits the process, prints, or mutates the report: what to do with
a failing gate is the caller's decision.
"""

from __future__ import annotations as _annotations

import json as _json
from collections.abc import Iterator as _Iterator, Mapping as _Mapping, Sequence as _Sequence
from dataclasses import dataclass as _dataclass, field as _field
from typing import TYPE_CHECKING, Any as _Any

from ._report_schema import _QUALITY_DIMENSIONS
from ._rounding import _r2

if TYPE_CHECKING:  # pragma: no cover - import cycle only matters to type checkers
    from ._report import ProfileReport

#: Scores and null percentages live on a 0..=100 scale, and so do the
#: thresholds compared against them.
_PERCENTAGE_MAX = 100.0

#: What the requirements in a policy are statements about.
_SCOPES = ("full_source", "observed")

#: The component labels each dimension is built from. Uniqueness is the one
#: dimension with two, and they can differ in provenance: a full-stream row
#: tracker counts duplicates over every row while the key scan reads the
#: retained sample.
_DIMENSION_COMPONENTS: dict[str, tuple[str, ...]] = {
    "completeness": ("completeness",),
    "consistency": ("consistency",),
    "uniqueness": ("key_uniqueness", "duplicate_rows"),
    "accuracy": ("accuracy",),
    "timeliness": ("timeliness",),
    "validity": ("validity",),
    "precision": ("precision",),
}


def _complete() -> dict[str, str]:
    """A fresh dict each time: a shared constant would alias into every
    check, where a caller reading ``check.evidence`` could mutate it.
    """
    return {"coverage": "complete"}


def _incomplete(gap: str) -> dict[str, str]:
    return {"coverage": "incomplete", "reason": gap}


@_dataclass(frozen=True)
class QualityCheck:
    """One requirement and what the report said about it.

    ``message`` is deliberately free of numbers: ``observed`` and ``expected``
    carry those, so the prose is identical wherever a value would have been
    formatted and the Rust and Python implementations cannot drift on rounding
    inside a string.
    """

    #: Stable identifier for the requirement, e.g. ``"max_null_percentage"``.
    code: str
    #: The constraint applied: ``{"comparison": ..., "value": ...}``, or
    #: ``{"comparison": "analyzed"}`` when no number is compared.
    expected: dict[str, _Any]
    #: What the requirement is a statement about.
    scope: str
    #: Whether the data behind ``observed`` covers the whole source.
    evidence: dict[str, _Any]
    #: ``"passed"``, ``"failed"``, or ``"not_evaluated"``.
    status: str
    #: A fixed sentence naming the outcome.
    message: str
    #: The column the requirement concerns, when it concerns one.
    column: str | None = None
    #: The quality dimension the requirement concerns, when it concerns one.
    dimension: str | None = None
    #: The value read from the report, ``None`` when nothing was read.
    observed: float | int | None = None
    #: Why the check was not evaluated, when it was not.
    reason: dict[str, _Any] | None = None
    #: Where the whole-source value lies, when ``observed`` came from a
    #: retained sample and the report bounds it: ``lower``, ``upper`` and
    #: ``confidence_level``. A ``full_source`` requirement is decided on this
    #: interval rather than on ``observed``.
    bounds: dict[str, _Any] | None = None

    @property
    def is_violation(self) -> bool:
        """True when the check was evaluated and violated."""
        return self.status == "failed"

    def to_dict(self) -> dict[str, _Any]:
        """The check as a JSON-ready document."""
        document: dict[str, _Any] = {"code": self.code}
        if self.column is not None:
            document["column"] = self.column
        if self.dimension is not None:
            document["dimension"] = self.dimension
        document["expected"] = dict(self.expected)
        if self.observed is not None:
            document["observed"] = self.observed
        document["scope"] = self.scope
        document["evidence"] = dict(self.evidence)
        if self.bounds is not None:
            document["bounds"] = dict(self.bounds)
        document["status"] = self.status
        if self.reason is not None:
            document.update(self.reason)
        document["message"] = self.message
        return document


@_dataclass(frozen=True)
class QualityGateResult:
    """The structured result of evaluating a policy against a report.

    The verdict is three-valued, because a decision and the evidence behind it
    are separate facts:

    ``"fail"``
        At least one requirement was conclusively violated.
    ``"inconclusive"``
        Nothing was violated, and at least one requirement could not be
        evaluated — a metric was not analyzed, a column was not profiled, or
        the evidence does not reach as far as the requirement does.
    ``"pass"``
        Every requirement was evaluated and met.

    ``"fail"`` wins over ``"inconclusive"``: a witnessed violation is a
    decision.
    """

    verdict: str
    scope: str
    evidence: dict[str, _Any]
    checks: tuple[QualityCheck, ...] = _field(default_factory=tuple)

    @property
    def passed(self) -> bool:
        """True only for ``verdict == "pass"``.

        An inconclusive result is not a pass: reading it as one is the mistake
        this API exists to prevent. Read :attr:`verdict` to tell "violated"
        from "could not tell".
        """
        return self.verdict == "pass"

    @property
    def violations(self) -> list[QualityCheck]:
        """The checks that were evaluated and violated."""
        return [check for check in self.checks if check.is_violation]

    @property
    def unevaluated(self) -> list[QualityCheck]:
        """The checks that could not be evaluated."""
        return [check for check in self.checks if check.status == "not_evaluated"]

    def __bool__(self) -> bool:
        return self.passed

    def __iter__(self) -> _Iterator[QualityCheck]:
        return iter(self.checks)

    def to_dict(self) -> dict[str, _Any]:
        """The result as a JSON-ready document, as the Rust layer writes it."""
        return {
            "verdict": self.verdict,
            "scope": self.scope,
            "evidence": dict(self.evidence),
            "checks": [check.to_dict() for check in self.checks],
        }

    def to_json(self, indent: int = 2) -> str:
        """The result as a JSON string."""
        return _json.dumps(self.to_dict(), indent=indent)


def _real_number(value: _Any) -> float:
    """The value as a float, or NaN when it is not a real number.

    Text is refused even when it spells a number: the Rust layer takes an
    ``f64``, and a quoted threshold in a policy file is more likely a mistake
    than an intent. ``bool`` is refused because ``True`` is not a threshold.
    Every other real number converts, numpy scalars and ``Decimal`` included,
    and one too large for a float is refused rather than raising
    ``OverflowError`` past a caller that catches ``ValueError``.
    """
    if isinstance(value, (bool, str, bytes, bytearray)):
        return float("nan")
    try:
        return float(value)
    except (TypeError, ValueError, OverflowError):
        return float("nan")


def _percentage(code: str, subject: str | None, value: _Any) -> float:
    """Accept a 0..=100 threshold, rejecting anything else by name.

    An unsatisfiable threshold is a configuration mistake, and reporting it as
    a failed gate would blame the data for it.
    """
    number = _real_number(value)
    if number != number or not 0.0 <= number <= _PERCENTAGE_MAX:
        named = f" for `{subject}`" if subject else ""
        raise ValueError(
            f"{code} threshold{named} must be a percentage between 0 and "
            f"{_PERCENTAGE_MAX:g}, got {value!r}"
        )
    return number


def _count(code: str, value: _Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{code} must be a non-negative whole number, got {value!r}")
    return value


def _dimension_name(name: _Any, *, setting: str) -> str:
    if name not in _QUALITY_DIMENSIONS:
        valid = ", ".join(_QUALITY_DIMENSIONS)
        raise ValueError(f"{setting}: unknown quality dimension {name!r}. Valid: {valid}")
    return str(name)


class _Policy:
    """The evaluator. Constructed from :meth:`ProfileReport.check` keywords."""

    def __init__(
        self,
        *,
        min_quality_score: float | None,
        min_dimension_scores: _Mapping[str, float] | None,
        max_null_percentage: _Mapping[str, float] | float | None,
        max_duplicate_rows: int | None,
        require_metrics: _Sequence[str] | None,
        scope: str,
    ) -> None:
        if scope not in _SCOPES:
            raise ValueError(f"scope must be one of {_SCOPES}, got {scope!r}")
        self.scope = scope

        self.min_quality_score = (
            None
            if min_quality_score is None
            else _percentage("min_quality_score", None, min_quality_score)
        )

        self.min_dimension_scores: dict[str, float] = {}
        for name, value in (min_dimension_scores or {}).items():
            dimension = _dimension_name(name, setting="min_dimension_scores")
            self.min_dimension_scores[dimension] = _percentage(
                "min_dimension_score", dimension, value
            )

        self.max_null_percentage: dict[str, float] = {}
        self.max_null_percentage_any: float | None = None
        if isinstance(max_null_percentage, _Mapping):
            for column, value in max_null_percentage.items():
                limit = _percentage(
                    "max_null_percentage", None if column == "*" else str(column), value
                )
                if column == "*":
                    self.max_null_percentage_any = limit
                else:
                    self.max_null_percentage[str(column)] = limit
        elif max_null_percentage is not None:
            self.max_null_percentage_any = _percentage(
                "max_null_percentage", None, max_null_percentage
            )

        self.max_duplicate_rows = (
            None if max_duplicate_rows is None else _count("max_duplicate_rows", max_duplicate_rows)
        )

        self.require_quality = False
        self.require_dimensions: set[str] = set()
        for metric in require_metrics or ():
            if metric == "quality":
                self.require_quality = True
            else:
                self.require_dimensions.add(_dimension_name(metric, setting="require_metrics"))

        if not (
            self.min_quality_score is not None
            or self.min_dimension_scores
            or self.max_null_percentage
            or self.max_null_percentage_any is not None
            or self.max_duplicate_rows is not None
            or self.require_quality
            or self.require_dimensions
        ):
            raise ValueError("check() states no requirement; a gate must check something")

    # -- evaluation ---------------------------------------------------------

    def evaluate(self, report: ProfileReport) -> QualityGateResult:
        scan = _scan_evidence(report)
        checks: list[QualityCheck] = []

        if self.require_quality:
            checks.append(self._require_metric(report, None))
        for dimension in _QUALITY_DIMENSIONS:
            if dimension in self.require_dimensions:
                checks.append(self._require_metric(report, dimension))

        if self.min_quality_score is not None:
            checks.append(self._quality_score(report, scan, self.min_quality_score))
        for dimension in _QUALITY_DIMENSIONS:
            minimum = self.min_dimension_scores.get(dimension)
            if minimum is not None:
                checks.append(self._dimension_score(report, scan, dimension, minimum))

        checks.extend(self._null_percentages(report, scan))

        if self.max_duplicate_rows is not None:
            checks.append(self._duplicate_rows(report, scan, self.max_duplicate_rows))

        if any(check.is_violation for check in checks):
            verdict = "fail"
        elif any(check.status == "not_evaluated" for check in checks):
            verdict = "inconclusive"
        else:
            verdict = "pass"
        return QualityGateResult(
            verdict=verdict, scope=self.scope, evidence=scan, checks=tuple(checks)
        )

    def _decide_aggregate(self, evidence: dict[str, _Any], satisfied: bool) -> str:
        """Decide a comparison whose observed value is an average or a ratio.

        Such a value, computed over part of a source, bounds nothing about the
        rest of it, so under ``full_source`` scope incomplete evidence leaves
        the requirement unevaluated in both directions.
        """
        if self.scope == "full_source" and evidence["coverage"] == "incomplete":
            return "not_evaluated"
        return "passed" if satisfied else "failed"

    def _require_metric(self, report: ProfileReport, dimension: str | None) -> QualityCheck:
        # Availability is a property of the report, not of how much of the
        # source it covers: a sampled run still either analyzed the metric or
        # did not. So this check never consults evidence.
        quality = report.quality
        if quality is None:
            analyzed = False
        elif dimension is None:
            analyzed = True
        else:
            analyzed = quality.dimension_scores().get(dimension) is not None
        if dimension is None:
            message = (
                "quality was analyzed" if analyzed else "the report carries no quality assessment"
            )
        else:
            message = (
                "the required dimension was assessed"
                if analyzed
                else "the required dimension was not assessed"
            )
        return QualityCheck(
            code="require_metric",
            dimension=dimension,
            expected={"comparison": "analyzed"},
            scope=self.scope,
            evidence=_complete(),
            status="passed" if analyzed else "failed",
            message=message,
        )

    def _quality_score(
        self, report: ProfileReport, scan: dict[str, _Any], minimum: float
    ) -> QualityCheck:
        evidence = _weaker(scan, _quality_evidence(report, None))
        expected = {"comparison": "at_least", "value": _r2(minimum)}
        quality = report.quality
        if quality is None:
            return _unavailable_quality(
                report,
                code="min_quality_score",
                expected=expected,
                scope=self.scope,
                evidence=evidence,
            )
        score = quality.overall_quality_score()
        if score is None:
            return QualityCheck(
                code="min_quality_score",
                expected=expected,
                scope=self.scope,
                evidence=evidence,
                status="not_evaluated",
                reason={"reason": "not_assessed"},
                message=(
                    "no quality dimension had anything to assess, so there is no overall score"
                ),
            )
        bounds = _check_bounds(report, None)
        status, bounds, message = self._decide_score(
            evidence, score, minimum, bounds, "the overall quality score"
        )
        return QualityCheck(
            code="min_quality_score",
            expected=expected,
            observed=_r2(score),
            scope=self.scope,
            evidence=evidence,
            bounds=bounds,
            status=status,
            reason=_evidence_reason(status, evidence),
            message=message,
        )

    def _dimension_score(
        self, report: ProfileReport, scan: dict[str, _Any], dimension: str, minimum: float
    ) -> QualityCheck:
        evidence = _weaker(scan, _quality_evidence(report, dimension))
        expected = {"comparison": "at_least", "value": _r2(minimum)}
        quality = report.quality
        if quality is None:
            return _unavailable_quality(
                report,
                code="min_dimension_score",
                expected=expected,
                scope=self.scope,
                evidence=evidence,
                dimension=dimension,
            )
        score = quality.dimension_scores().get(dimension)
        if score is None:
            return QualityCheck(
                code="min_dimension_score",
                dimension=dimension,
                expected=expected,
                scope=self.scope,
                evidence=evidence,
                status="not_evaluated",
                reason={"reason": "not_assessed"},
                message="this dimension had nothing to assess in this run",
            )
        bounds = _check_bounds(report, dimension)
        status, bounds, message = self._decide_score(
            evidence, score, minimum, bounds, "this dimension's score"
        )
        return QualityCheck(
            code="min_dimension_score",
            dimension=dimension,
            expected=expected,
            observed=_r2(score),
            scope=self.scope,
            evidence=evidence,
            bounds=bounds,
            status=status,
            reason=_evidence_reason(status, evidence),
            message=message,
        )

    def _decide_score(
        self,
        evidence: dict[str, _Any],
        score: float,
        minimum: float,
        bounds: dict[str, _Any] | None,
        subject: str,
    ) -> tuple[str, dict[str, _Any] | None, str]:
        """Decide a minimum on a score, on its whole-source interval when the
        only gap in the evidence is the quality sample and the report bounds
        the score. The interval settles the requirement when it lies wholly on
        one side of the minimum; a minimum inside it is left unevaluated.

        Returns the status, the interval the check records, and its message.
        """
        sampled = evidence.get("reason") == "quality_sampled"
        if bounds is not None and self.scope == "full_source" and sampled:
            if bounds["lower"] >= minimum:
                status = "passed"
            elif bounds["upper"] < minimum:
                status = "failed"
            else:
                status = "not_evaluated"
            recorded = {
                "lower": _r2(bounds["lower"]),
                "upper": _r2(bounds["upper"]),
                "confidence_level": bounds["confidence_level"],
            }
            return status, recorded, _bounded_message(status, subject)
        status = self._decide_aggregate(evidence, score >= minimum)
        return status, None, _aggregate_message(status, subject)

    def _null_percentages(self, report: ProfileReport, scan: dict[str, _Any]) -> list[QualityCheck]:
        """One check per named column in column-name order, then one per
        remaining profiled column when a wildcard limit is set.

        The order is fixed by the data rather than by how the policy was
        written, so a policy read out of a JSON object still evaluates
        identically on both layers.
        """
        # First wins on a duplicate name, matching the Rust evaluator's
        # `find`. A dict comprehension would keep the last one instead.
        profiles: dict[str, _Any] = {}
        for profile in report.profiles:
            profiles.setdefault(profile.name, profile)
        checks = [
            self._null_percentage(column, profiles.get(column), scan, limit)
            for column, limit in sorted(self.max_null_percentage.items())
        ]
        if self.max_null_percentage_any is not None:
            checks.extend(
                self._null_percentage(profile.name, profile, scan, self.max_null_percentage_any)
                for profile in report.profiles
                if profile.name not in self.max_null_percentage
            )
        return checks

    def _null_percentage(
        self, column: str, profile: _Any, scan: dict[str, _Any], limit: float
    ) -> QualityCheck:
        # Null counts accumulate over every scanned row rather than over the
        # retained quality sample, so only the scan's own gap applies here.
        expected = {"comparison": "at_most", "value": _r2(limit)}
        if profile is None:
            return QualityCheck(
                code="max_null_percentage",
                column=column,
                expected=expected,
                scope=self.scope,
                evidence=scan,
                status="not_evaluated",
                reason={"reason": "column_not_profiled"},
                message="this column has no profile in the report",
            )
        percentage = profile.null_percentage
        if percentage is None:
            # No value was read for the column, so "what share of its values
            # are null" has no answer. Zero rows is not zero percent.
            return QualityCheck(
                code="max_null_percentage",
                column=column,
                expected=expected,
                scope=self.scope,
                evidence=scan,
                status="not_evaluated",
                reason={"reason": "not_assessed"},
                message="no values were read for this column",
            )
        status = self._decide_aggregate(scan, percentage <= limit)
        messages = {
            "passed": "this column's null percentage is within the allowance",
            "failed": "this column's null percentage is above the allowance",
            "not_evaluated": (
                "the scan does not cover the whole source, and a null percentage over "
                "part of it bounds nothing about the rest"
            ),
        }
        return QualityCheck(
            code="max_null_percentage",
            column=column,
            expected=expected,
            observed=_r2(percentage),
            scope=self.scope,
            evidence=scan,
            status=status,
            reason=_evidence_reason(status, scan),
            message=messages[status],
        )

    def _duplicate_rows(
        self, report: ProfileReport, scan: dict[str, _Any], limit: int
    ) -> QualityCheck:
        """A duplicate count is the one requirement here that an incomplete
        scan can still settle in one direction: rows already witnessed as
        duplicates do not stop being duplicates when more rows are read, so an
        exact count above the allowance is a conclusive failure. A count at or
        below it is not a pass. An estimated count witnesses nothing.
        """
        evidence = _weaker(scan, _quality_evidence(report, component="duplicate_rows"))
        expected = {"comparison": "at_most", "value": limit}
        quality = report.quality
        if quality is None:
            return _unavailable_quality(
                report,
                code="max_duplicate_rows",
                expected=expected,
                scope=self.scope,
                evidence=evidence,
                dimension="uniqueness",
            )
        uniqueness = quality.uniqueness
        if not uniqueness or not uniqueness.get("rows_checked"):
            return QualityCheck(
                code="max_duplicate_rows",
                dimension="uniqueness",
                expected=expected,
                scope=self.scope,
                evidence=evidence,
                status="not_evaluated",
                reason={"reason": "not_assessed"},
                message="no rows were scanned for duplicates in this run",
            )
        observed = int(uniqueness["duplicate_rows"])
        estimated = bool(uniqueness.get("duplicate_rows_approximate"))
        exceeded = observed > limit
        incomplete = evidence["coverage"] == "incomplete"
        if self.scope == "full_source" and incomplete:
            # A witnessed violation is a decision, whatever the scan missed.
            status = "failed" if exceeded and not estimated else "not_evaluated"
        else:
            status = "failed" if exceeded else "passed"
        if status == "passed":
            message = "the duplicate-row count is within the allowance"
        elif status == "failed":
            message = (
                "the estimated duplicate-row count is above the allowance"
                if estimated
                else "duplicate rows were observed above the allowance"
            )
        elif estimated:
            message = (
                "the duplicate-row count is estimated, so it witnesses nothing about "
                "the rows that were not read"
            )
        else:
            message = (
                "no duplicate above the allowance was observed, and the rows that were "
                "not read may hold more"
            )
        return QualityCheck(
            code="max_duplicate_rows",
            dimension="uniqueness",
            expected=expected,
            observed=observed,
            scope=self.scope,
            evidence=evidence,
            status=status,
            reason=_evidence_reason(status, evidence),
            message=message,
        )


def _aggregate_message(status: str, subject: str) -> str:
    if status == "passed":
        return f"{subject} meets the required minimum"
    if status == "failed":
        return f"{subject} is below the required minimum"
    return f"{subject} was computed over part of the source, which bounds nothing about the rest"


def _bounded_message(status: str, subject: str) -> str:
    if status == "passed":
        return (
            f"{subject} was computed over a sample, and its whole-source interval meets the "
            "required minimum"
        )
    if status == "failed":
        return (
            f"{subject} was computed over a sample, and its whole-source interval is below the "
            "required minimum"
        )
    return (
        f"{subject} was computed over a sample, and the required minimum lies within its "
        "whole-source interval"
    )


def _check_bounds(report: ProfileReport, dimension: str | None) -> dict[str, _Any] | None:
    """The interval a score check decides on, from the report's bounds: the
    overall score's when ``dimension`` is ``None``, else that dimension's.
    """
    bounds = report.quality_score_bounds
    if bounds is None:
        return None
    if dimension is None:
        interval = bounds.get("overall_score")
    else:
        interval = (bounds.get("dimension_scores") or {}).get(dimension)
    if interval is None:
        return None
    return {
        "lower": interval["lower"],
        "upper": interval["upper"],
        "confidence_level": bounds["confidence_level"],
    }


def _evidence_reason(status: str, evidence: dict[str, _Any]) -> dict[str, _Any] | None:
    """The unevaluated-reason payload for a check blocked by its evidence."""
    if status != "not_evaluated":
        return None
    return {"reason": "evidence_incomplete", "gap": evidence["reason"]}


def _unavailable_quality(
    report: ProfileReport,
    *,
    code: str,
    expected: dict[str, _Any],
    scope: str,
    evidence: dict[str, _Any],
    dimension: str | None = None,
) -> QualityCheck:
    """A check that had no quality assessment to read, naming the reason the
    report recorded. Without that, "you did not ask for this" and "this broke"
    reach a gate as the same absence.
    """
    messages = {
        "not_requested": "quality metrics were not requested for this run",
        "no_data": "quality was requested but no sample was available to measure",
        "withheld_by_projection": (
            "quality was withheld: the requested dimensions measure whole rows and "
            "only some columns were profiled"
        ),
        "failed": "the quality computation failed",
    }
    status = report.quality_status
    return QualityCheck(
        code=code,
        dimension=dimension,
        expected=expected,
        scope=scope,
        evidence=evidence,
        status="not_evaluated",
        reason={"reason": "quality_unavailable", "quality_status": status},
        message=messages.get(status, "the report carries no quality assessment"),
    )


def _weaker(left: dict[str, _Any], right: dict[str, _Any]) -> dict[str, _Any]:
    """The weaker of two evidence statements: a number is only as complete as
    the least complete input behind it. The left-hand gap wins a tie, so the
    scan's gap is named ahead of the quality sample's.
    """
    return right if left["coverage"] == "complete" else left


def _scan_evidence(report: ProfileReport) -> dict[str, _Any]:
    """How much of the source the scan itself covered.

    Truncation is named ahead of sampling and sampling ahead of skipped
    records, so the gap reported is the largest one.
    """
    if not report.source_exhausted or report.truncation_reason is not None:
        return _incomplete("truncated")
    if report.sampling_applied:
        return _incomplete("sampled")
    if report.error_count:
        return _incomplete("records_skipped")
    return _complete()


def _quality_evidence(
    report: ProfileReport,
    dimension: str | None = None,
    *,
    component: str | None = None,
) -> dict[str, _Any]:
    """Whether the number a check reads came from every scanned row or from a
    retained sample of them, resolved per component.
    """
    if report.quality is None:
        # No assessment to read; the check that called this reports the
        # absence itself, and there is no number for evidence to describe.
        return _complete()
    sampled = report.quality_sampled_dimensions
    if sampled is None:
        # A report loaded from a document written before dataprof recorded
        # this does not say how its quality numbers were obtained. Unknown
        # coverage is not full coverage.
        return _incomplete("coverage_unrecorded")
    if component is not None:
        return _incomplete("quality_sampled") if component in sampled else _complete()
    if dimension is None:
        # The overall score is a weighted average over the *assessed*
        # dimensions, so a sampled dimension the weights exclude does not reach
        # it. Reporting the aggregate as sampled because of one would withhold
        # a verdict the number does not depend on.
        contributing = {
            label
            for assessed in report.quality.assessed_dimensions()
            for label in _DIMENSION_COMPONENTS[assessed]
        }
        matched = any(label in sampled for label in contributing)
    else:
        matched = any(label in sampled for label in _DIMENSION_COMPONENTS[dimension])
    return _incomplete("quality_sampled") if matched else _complete()
