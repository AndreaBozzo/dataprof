"""Structured, prioritized findings over a :class:`~dataprof.ProfileReport`.

This mirrors ``dataprof_runtime::findings`` in Rust. Both layers apply the same
rules in the same order and produce the same document; the shared fixture in
``tests/fixtures/findings_parity.json`` asserts it from both sides, so changing
one implementation alone fails that layer's test.

Findings are interpretation, not data cleaning. Each one names what was
observed and the evidence behind it, and none carries a raw cell value. They
are derived on demand and are not part of the serialized report, so a report
loaded from its document yields the same findings as the one that wrote it.
"""

from __future__ import annotations as _annotations

import json as _json
from collections.abc import Iterator as _Iterator
from dataclasses import dataclass as _dataclass, field as _field
from typing import TYPE_CHECKING, Any as _Any, NoReturn as _NoReturn

from ._columns import _homogeneity_counts, _type_mixture
from ._gate import _real_number
from ._render import _MIXED_TYPE_PCT, _NULL_HEAVY_PCT
from ._rounding import _r2

if TYPE_CHECKING:  # pragma: no cover - import cycle only matters to type checkers
    from ._report import ProfileReport

#: Pattern confidence a detection needs before a finding reports it: the
#: threshold validity scoring and the report summaries already use.
_MIN_PATTERN_CONFIDENCE = 0.5

#: Pattern categories that name personal or financial data.
_SENSITIVE_CATEGORIES = ("contact", "financial")

#: Identifier patterns that name a person. The rest of the identifier category
#: (UUIDs, product codes, VAT numbers) identifies things.
_PERSONAL_IDENTIFIER_PATTERNS = ("Codice Fiscale (IT)", "SSN (US)")

#: Severity per code. Warnings sort first.
_SEVERITY = {
    "all_null": "warning",
    "constant_column": "info",
    "duplicate_rows": "warning",
    "future_dates": "warning",
    "mixed_types": "warning",
    "null_heavy": "warning",
    "partial_scan": "info",
    "ragged_rows": "warning",
    "records_skipped": "warning",
    "sensitive_pattern": "info",
    "temporal_order_violations": "warning",
}
_SEVERITY_ORDER = ("warning", "info")

#: A fixed sentence per code. Deliberately free of numbers, as the gate's
#: messages are: the evidence carries those, so the Rust and Python layers
#: cannot drift on rounding inside a string.
_SUMMARY = {
    "all_null": "every value in this column is null",
    "constant_column": "every non-null value in this column is the same",
    "duplicate_rows": "the source contains exact duplicate rows",
    "future_dates": "some date values lie in the future",
    "mixed_types": "this column's values are split across lexical types",
    "null_heavy": "this column's null share is at or above the threshold",
    "partial_scan": "only part of the source was read; findings describe the rows that were",
    "ragged_rows": "some rows had a different field count from the header and were recovered",
    "records_skipped": "errors were counted while reading the source",
    "sensitive_pattern": "values in this column match a pattern for personal or financial data",
    "temporal_order_violations": "some start dates fall after their paired end dates",
}

_REASON_ORDER = (
    "quality_unavailable",
    "not_assessed",
    "estimated",
    "sampled",
    "unrecorded",
    "not_computed",
    "no_values",
)


@_dataclass(frozen=True)
class Finding:
    """Something in the report that deserves attention.

    ``summary`` is a fixed sentence per ``code``; ``evidence`` carries the
    metric values and thresholds that caused the finding, never a raw cell
    value.
    """

    #: Stable identifier for the rule, e.g. ``"null_heavy"``.
    code: str
    #: ``"warning"`` or ``"info"``.
    severity: str
    #: The metric values and thresholds behind the finding, by name.
    evidence: dict[str, _Any]
    #: A fixed sentence naming what was observed.
    summary: str
    #: The column it concerns, ``None`` for a finding about the whole report.
    column: str | None = None

    def to_dict(self) -> dict[str, _Any]:
        """The finding as a JSON-ready document, as the Rust layer writes it."""
        document: dict[str, _Any] = {"code": self.code, "severity": self.severity}
        if self.column is not None:
            document["column"] = self.column
        document["evidence"] = dict(self.evidence)
        document["summary"] = self.summary
        return document


@_dataclass(frozen=True)
class FindingsResult:
    """The findings a report supports, and the rules that could not look.

    An empty ``findings`` means "looked, found nothing" only for the rules not
    listed in ``not_evaluated``. A rule whose input the report does not carry,
    such as patterns never detected or quality not requested, produces no
    finding and is listed there with the reason. For that reason this object
    has no length, and ``bool()`` on it raises rather than answering: read the
    two fields.
    """

    #: Most severe first; within a severity by code, then report-level before
    #: column-level, then by column position, then by pattern name.
    findings: tuple[Finding, ...] = _field(default_factory=tuple)
    #: ``{"code", "reason", ...}`` per rule that could not be evaluated, by
    #: code then reason. A column rule lists the ``columns`` it could not look
    #: at, and was still evaluated for the others.
    not_evaluated: tuple[dict[str, _Any], ...] = _field(default_factory=tuple)

    def __iter__(self) -> _Iterator[Finding]:
        return iter(self.findings)

    def __bool__(self) -> _NoReturn:
        # Every object is truthy by default, so `if not report.findings():`
        # would never fire, and a falsy answer for "no findings" would read a
        # rule that could not look as clean. Neither is a safe default.
        raise TypeError(
            "a FindingsResult has no truth value: test `result.findings` for what "
            "was found and `result.not_evaluated` for what could not be checked"
        )

    def to_dict(self) -> dict[str, _Any]:
        """The result as a JSON-ready document, as the Rust layer writes it."""
        return {
            "findings": [finding.to_dict() for finding in self.findings],
            "not_evaluated": [dict(entry) for entry in self.not_evaluated],
        }

    def to_json(self, indent: int = 2) -> str:
        """The result as a JSON string."""
        return _json.dumps(self.to_dict(), indent=indent)


def _threshold(setting: str, value: _Any) -> float:
    """Accept a percentage in ``(0, 100]`` and return it at 2dp.

    The threshold is applied at the precision a percentage is compared and
    reported at, so the evidence states exactly what was compared. It is
    checked at that precision too: a value that rounds to 0 would report every
    column. Percentages are on the report's 0..100 scale, not 0..1 ratios.
    """
    applied = _r2(_real_number(value))
    if applied is None or not 0.0 < applied <= 100.0:
        raise ValueError(
            f"{setting} must be a percentage above 0 and at most 100 at two decimal "
            f"places, got {value!r}"
        )
    return applied


class _Collector:
    def __init__(self) -> None:
        self.ranked: list[tuple[tuple[int, str, int, str], Finding]] = []
        self.not_evaluated: list[dict[str, _Any]] = []

    def add(
        self,
        code: str,
        evidence: dict[str, _Any],
        *,
        column: str | None = None,
        index: int | None = None,
        detail: str = "",
    ) -> None:
        severity = _SEVERITY[code]
        finding = Finding(
            code=code,
            severity=severity,
            column=column,
            evidence=dict(sorted(evidence.items())),
            summary=_SUMMARY[code],
        )
        # Report-level findings (no index) sort before column findings.
        position = -1 if index is None else index
        key = (_SEVERITY_ORDER.index(severity), code, position, detail)
        self.ranked.append((key, finding))

    def skip_report(self, code: str, reason: dict[str, _Any]) -> None:
        self.not_evaluated.append({"code": code, **reason})

    def skip(self, code: str, reason: str, column: str) -> None:
        """Record a column a rule could not look at, joining the entry for the
        same rule and reason so each pair is listed once.
        """
        for entry in self.not_evaluated:
            if entry["code"] == code and entry["reason"] == reason:
                entry["columns"].append(column)
                return
        self.not_evaluated.append({"code": code, "reason": reason, "columns": [column]})

    def finish(self) -> FindingsResult:
        self.ranked.sort(key=lambda item: item[0])
        # Stable, so the columns inside one entry keep report order.
        self.not_evaluated.sort(
            key=lambda entry: (entry["code"], _REASON_ORDER.index(entry["reason"]))
        )
        return FindingsResult(
            findings=tuple(finding for _, finding in self.ranked),
            not_evaluated=tuple(self.not_evaluated),
        )


class _FindingPolicy:
    """The evaluator. Constructed from :meth:`ProfileReport.findings` keywords."""

    def __init__(
        self,
        *,
        null_heavy_percentage: float | None,
        mixed_types_percentage: float | None,
    ) -> None:
        self.null_heavy_percentage = (
            _threshold("null_heavy_percentage", _NULL_HEAVY_PCT)
            if null_heavy_percentage is None
            else _threshold("null_heavy_percentage", null_heavy_percentage)
        )
        self.mixed_types_percentage = (
            _threshold("mixed_types_percentage", _MIXED_TYPE_PCT)
            if mixed_types_percentage is None
            else _threshold("mixed_types_percentage", mixed_types_percentage)
        )

    def evaluate(self, report: ProfileReport) -> FindingsResult:
        out = _Collector()
        _scan_findings(report, out)
        _quality_findings(report, out)
        for index, column in enumerate(report.profiles):
            self._column_findings(index, column, out)
        return out.finish()

    def _column_findings(self, index: int, column: _Any, out: _Collector) -> None:
        name = column.name
        total = column.total_count
        nulls = column.null_count
        non_null = max(total - nulls, 0)

        if total == 0:
            out.skip("all_null", "no_values", name)
            out.skip("null_heavy", "no_values", name)
        elif non_null == 0:
            out.add(
                "all_null",
                {"null_count": nulls, "total_count": total},
                column=name,
                index=index,
            )
        else:
            # From the counts rather than the accessor's percentage, and
            # compared at the serialized precision, so a report read back from
            # its document lands on the same side of the threshold.
            percentage = _r2(nulls / total * 100.0)
            if percentage is not None and percentage >= self.null_heavy_percentage:
                out.add(
                    "null_heavy",
                    {
                        "null_percentage": percentage,
                        "threshold": self.null_heavy_percentage,
                    },
                    column=name,
                    index=index,
                )

        unique = column.unique_count
        if unique is None:
            out.skip("constant_column", "not_computed", name)
        elif non_null == 0:
            out.skip("constant_column", "no_values", name)
        elif unique == 1 and non_null > 1:
            # One value seen once is not a constant, just a single value.
            out.add(
                "constant_column",
                {"unique_count": 1, "non_null_count": non_null},
                column=name,
                index=index,
            )

        self._mixed_types(index, column, non_null, out)
        _sensitive_patterns(index, column, non_null, out)

    def _mixed_types(self, index: int, column: _Any, non_null: int, out: _Collector) -> None:
        name = column.name
        # Mixing forms ("A1", "123") is what an identifier scheme does, not a
        # defect: the same exemption the consistency dimension makes.
        if column.data_type == "identifier":
            return
        counts = _homogeneity_counts(column.type_homogeneity)
        if counts is None:
            out.skip("mixed_types", "not_computed", name)
            return
        classified = sum(counts.values())
        if classified == 0:
            out.skip("mixed_types", "no_values", name)
            return
        # Largest first, ties in declaration order, as the Rust layer's
        # `TypeHomogeneity::dominant` resolves them.
        dominant, dominant_count, _ = _type_mixture(column)[0]
        outside = classified - dominant_count
        if outside == 0:
            return
        outside_percentage = _r2(outside / classified * 100.0)
        if outside_percentage is None or outside_percentage < self.mixed_types_percentage:
            return
        out.add(
            "mixed_types",
            {
                "dominant_type": dominant,
                "dominant_percentage": _r2(dominant_count / classified * 100.0),
                "threshold": self.mixed_types_percentage,
                # Shares are counted over the values the profiler retained; a
                # classified count short of the non-null count says they were
                # sampled.
                "classified_count": classified,
                "non_null_count": non_null,
            },
            column=name,
            index=index,
        )


def _sensitive_patterns(index: int, column: _Any, non_null: int, out: _Collector) -> None:
    patterns = column.patterns
    if patterns is None:
        out.skip("sensitive_pattern", "not_computed", column.name)
        return
    if non_null == 0:
        # Detection ran over nothing, which is no evidence either way.
        out.skip("sensitive_pattern", "no_values", column.name)
        return
    for pattern in patterns:
        category = pattern.category
        if pattern.confidence < _MIN_PATTERN_CONFIDENCE or not (
            category in _SENSITIVE_CATEGORIES or pattern.name in _PERSONAL_IDENTIFIER_PATTERNS
        ):
            continue
        out.add(
            "sensitive_pattern",
            {
                "pattern": pattern.name,
                "category": category,
                "match_percentage": _r2(pattern.match_percentage),
            },
            column=column.name,
            index=index,
            detail=pattern.name,
        )


def _scan_findings(report: ProfileReport, out: _Collector) -> None:
    if not report.source_exhausted or report.truncation_reason is not None:
        partial = "truncated"
    elif report.sampling_applied:
        partial = "sampled"
    else:
        partial = None
    if partial is not None:
        out.add("partial_scan", {"reason": partial, "rows_processed": report.rows})
    if report._schema_version == 0:
        # Ragged rows were first counted by the release that introduced schema
        # versioning (0.10, #452). An older document loads with a defaulted
        # zero that was never measured.
        out.skip_report("ragged_rows", {"reason": "unrecorded"})
    elif report.ragged_row_count:
        out.add(
            "ragged_rows",
            {"ragged_row_count": report.ragged_row_count, "rows_processed": report.rows},
        )
    if report.error_count:
        out.add("records_skipped", {"error_count": report.error_count})


def _quality_findings(report: ProfileReport, out: _Collector) -> None:
    quality = report.quality
    if quality is None:
        unavailable = {"reason": "quality_unavailable", "quality_status": report.quality_status}
        for code in ("duplicate_rows", "future_dates", "temporal_order_violations"):
            out.skip_report(code, unavailable)
        return

    uniqueness = quality.uniqueness
    if not uniqueness or not uniqueness.get("rows_checked"):
        out.skip_report("duplicate_rows", {"reason": "not_assessed"})
    elif uniqueness.get("duplicate_rows_approximate"):
        out.skip_report("duplicate_rows", {"reason": "estimated"})
    elif uniqueness["duplicate_rows"] > 0:
        out.add(
            "duplicate_rows",
            {
                "duplicate_rows": int(uniqueness["duplicate_rows"]),
                "rows_checked": int(uniqueness["rows_checked"]),
            },
        )
    else:
        _skip_unless_complete(report, out, "duplicate_rows", "duplicate_rows")

    timeliness = quality.timeliness
    if not timeliness or not timeliness.get("date_values_checked"):
        out.skip_report("future_dates", {"reason": "not_assessed"})
    elif timeliness["future_dates_count"] > 0:
        out.add(
            "future_dates",
            {
                "future_dates_count": int(timeliness["future_dates_count"]),
                "date_values_checked": int(timeliness["date_values_checked"]),
            },
        )
    else:
        _skip_unless_complete(report, out, "future_dates", "timeliness")
    if not timeliness or not timeliness.get("temporal_pairs_checked"):
        out.skip_report("temporal_order_violations", {"reason": "not_assessed"})
    elif timeliness["temporal_violations"] > 0:
        out.add(
            "temporal_order_violations",
            {
                "temporal_violations": int(timeliness["temporal_violations"]),
                "temporal_pairs_checked": int(timeliness["temporal_pairs_checked"]),
            },
        )
    else:
        _skip_unless_complete(report, out, "temporal_order_violations", "timeliness")


def _skip_unless_complete(
    report: ProfileReport, out: _Collector, code: str, component: str
) -> None:
    """A zero count is a clean result only when it covers every scanned row.

    A count from the retained quality sample, or from a report that does not
    record where its counts came from, rules nothing out for the rows it did
    not see, so the rule is listed rather than read as clean. The component
    labels are the ones the quality gate resolves provenance by.
    """
    sampled = report.quality_sampled_dimensions
    if sampled is None:
        out.skip_report(code, {"reason": "unrecorded"})
    elif component in sampled:
        out.skip_report(code, {"reason": "sampled"})
