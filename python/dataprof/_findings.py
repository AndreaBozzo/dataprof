"""Structured, prioritized findings over a :class:`~dataprof.ProfileReport`.

The rules live in Rust, in ``dataprof_runtime::findings`` (#909). This module
turns the thresholds into the policy those rules apply and the document they
return into the classes below. A report loaded from a flat summary has no
native report behind it, so its facts are read from its accessors and handed
to the same rules. ``tests/fixtures/findings_parity.json`` states the expected
findings, and both layers assert them.

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

from ._columns import _homogeneity_counts
from ._dataprof import (
    findings_from_input_json as _findings_from_input_json,
    summary_unlisted_components as _summary_unlisted_components,
)
from ._gate import _real_number
from ._render import _MIXED_TYPE_PCT, _NULL_HEAVY_PCT
from ._rounding import _r2

if TYPE_CHECKING:  # pragma: no cover - import cycle only matters to type checkers
    from ._report import ProfileReport


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


def _evaluate(
    report: ProfileReport,
    *,
    null_heavy_percentage: float | None,
    mixed_types_percentage: float | None,
) -> FindingsResult:
    """The findings of ``report`` under these thresholds, from the Rust rules.

    A native report is evaluated as it is. A report loaded from a flat summary
    has no native report behind it, so the facts the rules read are taken from
    its accessors, which apply the summary's legacy defaults, and evaluated by
    the same rules.
    """
    null_heavy = _threshold(
        "null_heavy_percentage",
        _NULL_HEAVY_PCT if null_heavy_percentage is None else null_heavy_percentage,
    )
    mixed_types = _threshold(
        "mixed_types_percentage",
        _MIXED_TYPE_PCT if mixed_types_percentage is None else mixed_types_percentage,
    )
    native = report._native_report
    if native is not None:
        document = native.findings_json(null_heavy, mixed_types)
    else:
        facts = _json.dumps(_summary_facts(report))
        document = _findings_from_input_json(facts, null_heavy, mixed_types)
    return _result(_json.loads(document))


def _result(document: dict[str, _Any]) -> FindingsResult:
    return FindingsResult(
        findings=tuple(
            Finding(
                code=entry["code"],
                severity=entry["severity"],
                evidence=entry["evidence"],
                summary=entry["summary"],
                column=entry.get("column"),
            )
            for entry in document["findings"]
        ),
        not_evaluated=tuple(document["not_evaluated"]),
    )


def _count(value: _Any) -> int:
    """A recorded count; an absent one reads as zero, as the rules read it."""
    return int(value or 0)


def _summary_facts(report: ProfileReport) -> dict[str, _Any]:
    """What ``dataprof_runtime::FindingsInput`` reads, from a summary's accessors."""
    quality = report.quality
    # The summary as it would be written: it omits a sampling_applied the
    # loaded document did not record (#1236).
    summary = report._legacy_summary()
    return {
        "rows_processed": _count(report.rows),
        # A fact the summary does not record crosses as null, so the rule
        # that reads it lists itself as not evaluated (#1306, #1240). A
        # malformed value crosses as written and the Rust reader rejects it.
        "source_exhausted": report.source_exhausted,
        "truncated": report.truncation_reason is not None,
        "sampling_applied": summary["execution"].get("sampling_applied"),
        # A summary written before 0.10 did not count ragged rows, and one that
        # omits the field records none; the loader fills in a zero for both.
        "ragged_row_count": _count(report.ragged_row_count)
        if report._ragged_row_count_recorded
        else None,
        "unterminated_quote": report.unterminated_quote,
        "csv_source": _csv_source(report),
        "error_count": _count(report.error_count),
        # A flat summary records no source format, so a missing entry cannot
        # be told from a source that is not CSV: nothing is listed for it.
        "delimiter_detection": (report.csv_metadata or {}).get("delimiter_detection"),
        # Absent for a summary written before 0.13, whose key may be a guess
        # from column names (#1065).
        "key_column_source": (report.metric_semantics or {}).get("key_column_source"),
        "quality_status": report.quality_status,
        "quality": None
        if quality is None
        else {
            "uniqueness": _metric_facts(
                quality.uniqueness,
                counts=("rows_checked", "duplicate_rows"),
                flags=("duplicate_rows_approximate",),
                names=("key_column",),
            ),
            "timeliness": _metric_facts(
                quality.timeliness,
                counts=(
                    "date_values_checked",
                    "future_dates_count",
                    "temporal_pairs_checked",
                    "temporal_violations",
                ),
            ),
            "sampled_dimensions": report.quality_sampled_dimensions,
            # Read by the rule compare() applies to the summary: empty when
            # the summary records no exact list (#1027).
            "unlisted_components": _summary_unlisted_components(_json.dumps(summary["quality"])),
        },
        "columns": [_column_facts(column) for column in report.profiles],
    }


def _csv_source(report: ProfileReport) -> bool | None:
    """Whether the summarized source was read as CSV, ``None`` when unknown.

    A summary written before 0.14 records no format for a file, bytes or
    stream source, so whether a quote check applies cannot be told. A query
    or dataframe source has no text to quote.
    """
    source_format = report.source_format
    if source_format is not None:
        return source_format == "csv"
    if report.source_type in ("dataframe", "query"):
        return False
    return None


def _metric_facts(
    metrics: dict[str, _Any] | None,
    *,
    counts: tuple[str, ...],
    flags: tuple[str, ...] = (),
    names: tuple[str, ...] = (),
) -> dict[str, _Any] | None:
    if not metrics:
        return None
    facts: dict[str, _Any] = {name: _count(metrics.get(name)) for name in counts}
    facts.update({name: bool(metrics.get(name)) for name in flags})
    # A name stays absent when the summary records none.
    facts.update({name: metrics.get(name) for name in names})
    return facts


def _column_facts(column: _Any) -> dict[str, _Any]:
    patterns = column.patterns
    return {
        "name": column.name,
        "identifier": column.data_type == "identifier",
        "total_count": column.total_count,
        "null_count": column.null_count,
        "unique_count": column.unique_count,
        "unique_count_is_approximate": column.unique_count_is_approximate,
        "type_homogeneity": _homogeneity_counts(column.type_homogeneity),
        "locale_number_count": column.locale_number_count,
        "patterns": None
        if patterns is None
        else [
            {
                "name": pattern.name,
                "category": pattern.category,
                "confidence": pattern.confidence,
                "match_percentage": pattern.match_percentage,
            }
            for pattern in patterns
        ],
    }
