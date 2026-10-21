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


class _PolicyFieldError(ValueError):
    """A policy validation error identifying the invalid configuration key."""

    def __init__(self, key: str, message: str) -> None:
        super().__init__(message)
        self.key = key


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
    #: The baseline's side of a rule about change: its ``observed`` value,
    #: its ``evidence``, and ``change``, this check's ``observed`` minus the
    #: baseline's in points. ``None`` on every other rule.
    baseline: dict[str, _Any] | None = None

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
        if self.baseline is not None:
            document["baseline"] = dict(self.baseline)
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
    #: When the policy was evaluated against a baseline: ``reports``, with the
    #: baseline as ``a`` and this report as ``b``, ``schema``, the columns
    #: ``added``, ``removed`` and ``common``, and ``diagnostics``, the schema
    #: changes from the baseline, which never move the verdict. ``None``
    #: otherwise.
    baseline: dict[str, _Any] | None = None

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
        document: dict[str, _Any] = {
            "verdict": self.verdict,
            "scope": self.scope,
            "evidence": dict(self.evidence),
            "checks": [check.to_dict() for check in self.checks],
        }
        if self.baseline is not None:
            document["baseline"] = dict(self.baseline)
        return document

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


def _percentage(
    code: str,
    subject: str | None,
    value: _Any,
    *,
    policy_key: str | None = None,
) -> float:
    """Accept a 0..=100 threshold, rejecting anything else by name.

    An unsatisfiable threshold is a configuration mistake, and reporting it as
    a failed gate would blame the data for it.
    """
    number = _real_number(value)
    if number != number or not 0.0 <= number <= _PERCENTAGE_MAX:
        named = f" for `{subject}`" if subject else ""
        raise _PolicyFieldError(
            policy_key or code,
            f"{code} threshold{named} must be a percentage between 0 and "
            f"{_PERCENTAGE_MAX:g}, got {value!r}",
        )
    return number


#: The largest count the Rust gate accepts (`usize` on the 64-bit wheels).
_COUNT_MAX = (1 << 64) - 1


def _count(code: str, value: _Any) -> int:
    """Accept a whole-number limit, refusing one the Rust gate cannot take
    with ``ValueError`` rather than the ``OverflowError`` its conversion raises.
    """
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= _COUNT_MAX:
        raise _PolicyFieldError(
            code,
            f"{code} must be a whole number between 0 and {_COUNT_MAX}, got {value!r}",
        )
    return value


def _dimension_name(name: _Any, *, setting: str) -> str:
    if name not in _QUALITY_DIMENSIONS:
        valid = ", ".join(_QUALITY_DIMENSIONS)
        raise _PolicyFieldError(
            setting,
            f"{setting}: unknown quality dimension {name!r}. Valid: {valid}",
        )
    return str(name)


class _Policy:
    """The evaluator. Constructed from :meth:`ProfileReport.check` keywords."""

    def __init__(
        self,
        *,
        min_rows: int | None,
        min_quality_score: float | None,
        min_dimension_scores: _Mapping[str, float] | None,
        max_null_percentage: _Mapping[str, float] | float | None,
        max_duplicate_rows: int | None,
        require_metrics: _Sequence[str] | None,
        scope: str,
        baseline: _RustProfileReport | str | None = None,
        max_quality_score_drop: float | None = None,
        max_dimension_score_drops: _Mapping[str, float] | None = None,
        max_null_percentage_increase: _Mapping[str, float] | float | None = None,
        require_columns: _Sequence[str] | None = None,
        forbid_removed_columns: bool = False,
        forbid_added_columns: bool = False,
    ) -> None:
        if scope not in _SCOPES:
            raise _PolicyFieldError(
                "scope",
                f"scope must be one of {_SCOPES}, got {scope!r}",
            )
        self.scope = scope

        self.min_rows = None if min_rows is None else _count("min_rows", min_rows)

        self.min_quality_score = (
            None
            if min_quality_score is None
            else _percentage("min_quality_score", None, min_quality_score)
        )

        self.min_dimension_scores: dict[str, float] = {}
        for name, value in (min_dimension_scores or {}).items():
            dimension = _dimension_name(name, setting="min_dimension_scores")
            self.min_dimension_scores[dimension] = _percentage(
                "min_dimension_score",
                dimension,
                value,
                policy_key="min_dimension_scores",
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

        self.max_quality_score_drop = (
            None
            if max_quality_score_drop is None
            else _percentage("max_quality_score_drop", None, max_quality_score_drop)
        )

        self.max_dimension_score_drops: dict[str, float] = {}
        for name, value in (max_dimension_score_drops or {}).items():
            dimension = _dimension_name(name, setting="max_dimension_score_drops")
            self.max_dimension_score_drops[dimension] = _percentage(
                "max_dimension_score_drop",
                dimension,
                value,
                policy_key="max_dimension_score_drops",
            )

        self.max_null_percentage_increase: dict[str, float] = {}
        self.max_null_percentage_increase_any: float | None = None
        if isinstance(max_null_percentage_increase, _Mapping):
            for column, value in max_null_percentage_increase.items():
                limit = _percentage(
                    "max_null_percentage_increase",
                    None if column == "*" else str(column),
                    value,
                )
                if column == "*":
                    self.max_null_percentage_increase_any = limit
                else:
                    self.max_null_percentage_increase[str(column)] = limit
        elif max_null_percentage_increase is not None:
            self.max_null_percentage_increase_any = _percentage(
                "max_null_percentage_increase", None, max_null_percentage_increase
            )

        # A bare string is a sequence of characters, never a list of columns,
        # and a one-shot iterator would be consumed by the check below.
        if require_columns is not None and (
            not isinstance(require_columns, _Sequence)
            or isinstance(require_columns, (str, bytes))
            or not all(isinstance(column, str) for column in require_columns)
        ):
            raise _PolicyFieldError(
                "require_columns",
                f"require_columns must be a list of column names, got {require_columns!r}",
            )
        self.require_columns = sorted(set(require_columns or ()))
        for key, value in (
            ("forbid_removed_columns", forbid_removed_columns),
            ("forbid_added_columns", forbid_added_columns),
        ):
            if not isinstance(value, bool):
                raise _PolicyFieldError(key, f"{key} must be True or False, got {value!r}")
        self.forbid_removed_columns = forbid_removed_columns
        self.forbid_added_columns = forbid_added_columns

        self.baseline = baseline
        if baseline is None and self._has_change_rules():
            raise _PolicyFieldError(
                "baseline",
                "a rule about change (max_quality_score_drop, max_dimension_score_drops, "
                "max_null_percentage_increase, forbid_removed_columns, forbid_added_columns) "
                "needs a baseline report to compare against",
            )

        self.require_quality = False
        self.require_dimensions: set[str] = set()
        for metric in require_metrics or ():
            if metric == "quality":
                self.require_quality = True
            else:
                self.require_dimensions.add(_dimension_name(metric, setting="require_metrics"))

        if not (
            self.min_rows is not None
            or self.min_quality_score is not None
            or self.min_dimension_scores
            or self.max_null_percentage
            or self.max_null_percentage_any is not None
            or self.max_duplicate_rows is not None
            or self.require_quality
            or self.require_dimensions
            or self.require_columns
            or self._has_change_rules()
        ):
            raise ValueError("check() states no requirement; a gate must check something")

    def _has_change_rules(self) -> bool:
        return (
            self.max_quality_score_drop is not None
            or bool(self.max_dimension_score_drops)
            or bool(self.max_null_percentage_increase)
            or self.max_null_percentage_increase_any is not None
            or self.forbid_removed_columns
            or self.forbid_added_columns
        )

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
                min_rows=self.min_rows,
                min_quality_score=self.min_quality_score,
                min_dimension_scores=list(self.min_dimension_scores.items()),
                max_null_percentage=list(self.max_null_percentage.items()),
                max_null_percentage_any=self.max_null_percentage_any,
                max_duplicate_rows=self.max_duplicate_rows,
                require_quality=self.require_quality,
                require_dimensions=sorted(self.require_dimensions),
                scope=self.scope,
                baseline=self.baseline,
                max_quality_score_drop=self.max_quality_score_drop,
                max_dimension_score_drops=list(self.max_dimension_score_drops.items()),
                max_null_percentage_increase=list(self.max_null_percentage_increase.items()),
                max_null_percentage_increase_any=self.max_null_percentage_increase_any,
                require_columns=self.require_columns,
                forbid_removed_columns=self.forbid_removed_columns,
                forbid_added_columns=self.forbid_added_columns,
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
    "baseline",
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
        baseline=document.get("baseline"),
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
        baseline=document.get("baseline"),
    )
