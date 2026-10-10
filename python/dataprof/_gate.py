"""Declarative quality gates over a :class:`~dataprof.ProfileReport`.

The gate is evaluated by ``dataprof_runtime::quality_gate`` in Rust. This
module validates the keywords, names their errors the way Python callers expect,
and turns the Rust result document into the classes below. The shared fixture in
``tests/fixtures/quality_gate_parity.json`` asserts that document from both
languages.

Nothing here exits the process, prints, or mutates the report: what to do with
a failing gate is the caller's decision.
"""

from __future__ import annotations as _annotations

import json as _json
from collections.abc import Iterator as _Iterator, Mapping as _Mapping, Sequence as _Sequence
from dataclasses import dataclass as _dataclass, field as _field
from typing import TYPE_CHECKING, Any as _Any

from ._report_schema import _QUALITY_DIMENSIONS

if TYPE_CHECKING:  # pragma: no cover - import cycle only matters to type checkers
    from ._dataprof import ProfileReport as _RustProfileReport

#: Scores and null percentages live on a 0..=100 scale, and so do the
#: thresholds compared against them.
_PERCENTAGE_MAX = 100.0

#: What the requirements in a policy are statements about.
_SCOPES = ("full_source", "observed")


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


#: The largest count the Rust gate accepts (`usize` on the 64-bit wheels).
_COUNT_MAX = (1 << 64) - 1


def _count(code: str, value: _Any) -> int:
    """Accept a whole-number limit, refusing one the Rust gate cannot take
    with ``ValueError`` rather than the ``OverflowError`` its conversion raises.
    """
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= _COUNT_MAX:
        raise ValueError(f"{code} must be a whole number between 0 and {_COUNT_MAX}, got {value!r}")
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

    def evaluate(self, native: _RustProfileReport | None) -> QualityGateResult:
        """Evaluate against a report's runtime object, ``None`` for a report
        restored from a flat summary."""
        if native is None:
            # A flat summary (`to_dict()` output, or a file saved before 0.12)
            # is not a runtime report, and the gate evaluates runtime reports
            # only. Its missing provenance cannot be recovered by converting it.
            raise ValueError(
                "check() needs a report profiled by dataprof or loaded from the "
                "JSON that save() and to_json() write; this one was loaded from a "
                "flat to_dict() summary, which does not record the provenance the "
                "gate decides on. Profile the source again, or load its saved JSON."
            )
        document = _json.loads(
            native.quality_gate_json(
                min_quality_score=self.min_quality_score,
                min_dimension_scores=list(self.min_dimension_scores.items()),
                max_null_percentage=list(self.max_null_percentage.items()),
                max_null_percentage_any=self.max_null_percentage_any,
                max_duplicate_rows=self.max_duplicate_rows,
                require_quality=self.require_quality,
                require_dimensions=sorted(self.require_dimensions),
                scope=self.scope,
            )
        )
        return _result_from_document(document)


#: Keys of a Rust check document that map to a field of their own; every
#: other key belongs to the flattened not-evaluated reason.
_CHECK_FIELDS = (
    "code",
    "column",
    "dimension",
    "expected",
    "observed",
    "scope",
    "evidence",
    "bounds",
    "status",
    "message",
)


def _check_from_document(document: dict[str, _Any]) -> QualityCheck:
    reason = {key: value for key, value in document.items() if key not in _CHECK_FIELDS}
    return QualityCheck(
        code=document["code"],
        column=document.get("column"),
        dimension=document.get("dimension"),
        expected=document["expected"],
        observed=document.get("observed"),
        scope=document["scope"],
        evidence=document["evidence"],
        bounds=document.get("bounds"),
        status=document["status"],
        reason=reason or None,
        message=document["message"],
    )


def _result_from_document(document: dict[str, _Any]) -> QualityGateResult:
    return QualityGateResult(
        verdict=document["verdict"],
        scope=document["scope"],
        evidence=document["evidence"],
        checks=tuple(_check_from_document(check) for check in document["checks"]),
    )
