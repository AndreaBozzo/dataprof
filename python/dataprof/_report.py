"""Public report access, rendering, comparison, and persistence."""

from __future__ import annotations as _annotations

import copy as _copy
import csv as _csv
import functools as _functools
import html as _html
import json as _json
import os as _os
import tempfile as _tempfile
import warnings as _warnings
from collections.abc import (
    Callable as _Callable,
    Iterator as _Iterator,
    Mapping as _Mapping,
    Sequence as _Sequence,
)
from typing import Any as _Any, cast as _cast

from ._accessors import ColumnProfile, DataQualityMetrics, _NativeAccessor, _ReportView
from ._columns import _column_record, _dominant_pattern, column_to_dict
from ._dataprof import ProfileReport as _RustProfileReport, report_delta_json as _report_delta_json
from ._findings import FindingsResult as _FindingsResult, _evaluate as _evaluate_findings
from ._gate import QualityGateResult as _QualityGateResult, _Policy, _PolicyFieldError
from ._paths import _normalize_pathlike
from ._render import (
    _bounded,
    _column_flags,
    _estimate_tokens,
    _fit_section,
    _may_expose_values,
    _one_line,
    _pattern_cell,
    _pct_str,
    _section_min_cost,
    _stats_cell,
)
from ._report_backing import _report_from_dict
from ._report_schema import _QUALITY_DIMENSIONS, REPORT_SCHEMA_VERSION
from ._rounding import _r2, _r4, _round_dimension, _round_quartiles, _rstat


def _quality_status_document(state: str, error: str | None) -> dict[str, _Any]:
    """Serialize the quality-computation outcome.

    One object, one fact, the same shape the Rust dialect writes. ``error`` is
    present only on ``failed``, where it is the whole point of the record.
    """
    document: dict[str, _Any] = {"state": state}
    if error is not None:
        document["error"] = error
    return document


def _atomic_write(path: str, writer: _Callable[[str], None]) -> None:
    path = _os.path.realpath(path)
    directory = _os.path.dirname(_os.path.abspath(path))
    basename = _os.path.basename(path)

    fd, temp_path = _tempfile.mkstemp(
        dir=directory,
        prefix=f".{basename}.",
        suffix=".tmp",
    )
    _os.close(fd)

    try:
        writer(temp_path)

        try:
            mode = _os.stat(path).st_mode & 0o777
        except FileNotFoundError:
            mode_path = f"{temp_path}.mode"
            mode_fd = _os.open(
                mode_path,
                _os.O_WRONLY | _os.O_CREAT | _os.O_EXCL,
                0o666,
            )
            try:
                mode = _os.fstat(mode_fd).st_mode & 0o777
            finally:
                _os.close(mode_fd)
                _os.unlink(mode_path)

        _os.chmod(temp_path, mode)
        _os.replace(temp_path, path)
    finally:
        try:
            _os.unlink(temp_path)
        except FileNotFoundError:
            pass


def _atomic_write_text(
    path: str,
    content: str,
    newline: str | None = None,
) -> None:
    def write(temp_path: str) -> None:
        with open(temp_path, "w", encoding="utf-8", newline=newline) as f:
            f.write(content)

    _atomic_write(path, write)


def _check_source_version(source_version: object) -> None:
    """Refuse a ``source_version=`` that cannot identify anything, before any scan."""
    if source_version is None:
        return
    if not isinstance(source_version, str):
        raise TypeError(
            f"source_version must be a string or None, got {type(source_version).__name__}."
        )
    if not source_version.strip():
        raise ValueError(
            "source_version must not be empty: an empty version identifies no data. "
            "Pass None to record no version."
        )


class ProfileReport:
    """Report access and export methods shared by native and restored backings.

    Supports dict-like column access::

        report["column_name"]          # -> ColumnProfile
        "column_name" in report        # -> bool
        for name in report: ...        # iterate column names
        len(report)                    # number of columns

    Cross-engine numeric equality applies to the rounded metrics exported by
    ``to_dict()`` and ``to_json()``, for the same logical data, analysis options
    and analyzed population -- a sampled or truncated run is a different
    population, not a numeric difference. Native column float attributes retain
    full precision and may differ in their final digits with accumulation order.
    Loading a report preserves its serialized precision.
    """

    _MAX_REPR_COLUMNS = 15
    #: Columns the value-checks note names before it counts the rest.
    _MAX_NOTE_COLUMNS = 5

    def __init__(self, report: _RustProfileReport | _ReportView):
        self._native_report = None if isinstance(report, _ReportView) else report
        self._report = (
            report if isinstance(report, _ReportView) else _ReportView(_NativeAccessor(report))
        )

    # -- Property accessors --

    @property
    def id(self) -> str | None:
        """Report identifier, or None for a flat ``to_dict()`` summary.

        Flat summaries do not record an identifier, including current ones.
        """
        return self._report.id

    @property
    def timestamp(self) -> str | None:
        """Creation timestamp, or None for a flat ``to_dict()`` summary.

        Flat summaries do not record a timestamp, including current ones.
        """
        return self._report.timestamp

    @property
    def source(self) -> str:
        return self._report.source

    @property
    def source_type(self) -> str:
        return self._report.source_type

    @property
    def source_format(self) -> str | None:
        """The format a file, bytes or stream source was parsed as.

        ``"csv"``, ``"json"``, ``"jsonl"``, ``"parquet"``, or ``"arrow"`` for
        an Arrow C stream. ``None`` for a query or dataframe source, for a
        stream that recorded none, and for a summary written before 0.14.
        """
        return self._report.source_format

    @property
    def engine(self) -> str | None:
        return self._report.engine

    @property
    def recovery_events(self) -> list[dict[str, str]] | None:
        """Failed attempts and retries in order; None means history is unknown.

        An empty list means no recovery occurred. Each event records ``kind``,
        ``attempted``, ``retry`` and diagnostic ``error`` text.
        """
        events = self._report.recovery_events
        return None if events is None else [dict(event) for event in events]

    @property
    def rows(self) -> int:
        return self._report.rows_processed

    @property
    def columns(self) -> int:
        return self._report.columns_detected

    @_functools.cached_property
    def column_profiles(self) -> dict[str, ColumnProfile]:
        """Column profiles as a ``name → ColumnProfile`` mapping.

        Because this is a mapping, iterating it yields column *names* (like any
        dict), not profile objects — ``for c in report.column_profiles`` gives
        strings. To iterate the profiles themselves use :attr:`profiles`, and to
        look one up by name index into this mapping (``report["amount"]`` also
        works). When a dataset has duplicate column names one profile shadows
        the other here; :attr:`profiles` preserves the full ordered list.
        """
        cols = self._report.column_profiles
        d = {col.name: col for col in cols}
        if len(d) != len(cols):
            _warnings.warn(
                f"Dataset has duplicate column names — {len(cols) - len(d)} "
                "column(s) shadowed in dict access. Use .profiles "
                "for the full list.",
                stacklevel=2,
            )
        return d

    @property
    def profiles(self) -> list[ColumnProfile]:
        """The column profiles as an ordered list of :class:`ColumnProfile`.

        Use this when you want to iterate the profile objects rather than their
        names::

            for col in report.profiles:
                print(col.name, col.null_percentage)

        Unlike :attr:`column_profiles` (a name-keyed mapping), this preserves
        every column, including duplicate names.
        """
        return list(self._report.column_profiles)

    @property
    def quality_score(self) -> float | None:
        v = self._report.quality_score
        return _r2(v)

    @property
    def quality(self) -> DataQualityMetrics | None:
        return self._report.quality

    @property
    def quality_status(self) -> str:
        """What happened to the quality computation.

        ``quality is None`` alone cannot say whether quality was never asked
        for or was asked for and broke, so every report carries the reason:

        ``computed``
            Quality was computed; :attr:`quality` holds the assessment.
        ``not_requested``
            The quality pack was deselected for this run.
        ``no_data``
            Requested, but no quality sample was supplied to the assembler.
            An empty sample is analyzed and reports ``computed``.
        ``withheld_by_projection``
            Requested, but every requested dimension measures whole rows and
            the run profiled a subset of columns.
        ``failed``
            Requested and attempted; the computation failed. The message is in
            :attr:`quality_error`.
        ``unrecorded``
            Loaded from a document written before this field existed.
        """
        return self._report.quality_status

    @property
    def quality_error(self) -> str | None:
        """The error a failed quality computation reported, else ``None``.

        Non-``None`` exactly when :attr:`quality_status` is ``failed``.
        """
        return self._report.quality_error

    @property
    def quality_sampled_dimensions(self) -> list[str] | None:
        """Metric components computed from a retained sample of the scanned rows.

        ``None`` when the report carries no quality assessment, or when it was
        loaded from a document written before dataprof recorded this. An empty
        list means every computed component saw every scanned row; ``None`` is
        a third answer rather than a synonym for that empty list.

        A bounded quality sample is how a profiler keeps memory flat over a
        large source, so a fully scanned file can still carry sampled quality
        numbers: :attr:`source_exhausted` and :attr:`sampling_applied` do not
        answer this. Uniqueness appears as its two components,
        ``key_uniqueness`` and ``duplicate_rows``, which can differ in
        provenance.
        """
        sampled = self._report.quality_sampled_dimensions
        return None if sampled is None else list(sampled)

    @property
    def quality_score_bounds(self) -> dict[str, _Any] | None:
        """Where the whole-source quality scores lie, when some were computed
        over a retained sample of the scanned rows.

        A dict with ``confidence_level``, ``overall_score`` and
        ``dimension_scores``. Each interval is ``{"lower": ..., "upper": ...}``
        on the 0-100 scale, or ``None`` for a score with no bound: a duplicate
        scan over a sample, or an estimated count with no recorded floor, and
        an overall score that weighs one of them. Start/end ordering is
        compared only between date columns whose samples hold the same rows,
        so a pair where either column has nulls is not compared.

        The sample fixes each check: a column's type, dominant form, detected
        pattern, decimal scale and outlier fences. The intervals cover the
        scores those same checks give over every scanned value, and all
        of them hold at once with probability ``confidence_level``. An exact
        score's interval is the score itself.

        ``None`` when every score is exact, when there is no assessment, or
        when the report was loaded from a document that does not record
        bounds. A ``full_source`` quality gate decides a sampled score on this
        interval.
        """
        bounds = self._report.quality_score_bounds
        return None if bounds is None else _copy.deepcopy(dict(bounds))

    @property
    def quality_columns_without_value_checks(self) -> list[dict[str, str]] | None:
        """Columns whose values no value-level quality check read.

        Each entry is ``{"column": ..., "reason": ...}``, in column order. The
        one reason today is ``"nested"``: a struct, list or map column, whose
        values reach the checks only as a serialisation that differs by input
        path. Its nulls still count toward completeness and its values toward
        duplicate rows, but consistency, validity, accuracy, precision,
        timeliness and key uniqueness never read them, so those dimensions
        of :attr:`quality_score` say nothing about their values.

        An empty list means every column was checked. ``None`` when there is
        no quality assessment, or the report was loaded from a document
        written before 0.14, which did not record this.
        """
        columns = self._report.quality_columns_without_value_checks
        return None if columns is None else [dict(entry) for entry in columns]

    def _value_checks_note(self) -> str | None:
        """One line saying which columns the score's value checks skipped.

        ``None`` when every column was checked or the report does not say.
        """
        skipped = self.quality_columns_without_value_checks
        if not skipped:
            return None
        shown = [
            f"{_bounded(_one_line(entry['column']))} ({entry['reason']})"
            for entry in skipped[: self._MAX_NOTE_COLUMNS]
        ]
        if len(skipped) > len(shown):
            shown.append(f"+{len(skipped) - len(shown)} more")
        return (
            f"the quality score's value checks skipped {len(skipped)} of "
            f"{self.columns} columns: {', '.join(shown)}"
        )

    @property
    def metric_semantics(self) -> dict[str, str] | None:
        """How this report's measurements were defined.

        A dict of named definitions, such as
        ``{"text_length_unit": "unicode_scalar", "null_tokens": "common_markers",
        "accuracy_comparison": "exact_values"}``; the Python API reference
        lists every key.
        ``schema_version`` says whether a document validates, not whether two
        reports measured the same way: text lengths counted UTF-8 bytes through
        0.11; through 0.12 only empty, ``null`` and ``nan`` were nulls, pattern
        counts took every regex match, and skewness and kurtosis used another
        estimator, all with no change to the document's shape.

        ``None`` for a report loaded from a document written before dataprof
        recorded this, whose definitions are unknown rather than the current
        ones. A stored report keeps the value it was written with.
        """
        semantics = self._report.metric_semantics
        return None if semantics is None else dict(semantics)

    @property
    def analysis_settings(self) -> dict[str, _Any] | None:
        """The analysis settings this report was profiled with that change
        what a score measures on unchanged data and are recorded nowhere else
        in it.

        ``pattern_locale`` is the locale detected patterns were ranked with,
        which the validity score reads, as its ISO 3166-1 alpha-2 code
        (``"IT"``), or ``"none"`` when no locale was given. :meth:`compare`
        lists it under ``configuration``, and a baseline ``check()`` decides
        a validity or overall drop only when both reports agree on it.

        ``columns`` is the column selection: ``{"selection": "all"}``, or
        ``{"selection": "listed", "names": [...]}`` for a ``columns=``
        projection, in the order given. A baseline ``check()`` reads it to
        tell a column the source lacks from one that was not selected.

        ``metric_packs`` lists the packs the run computed, ``"schema"``
        always among them, in the order ``schema``, ``statistics``,
        ``patterns``, ``quality``. Without ``"patterns"`` validity is not
        assessed and the overall score averages the other dimensions. A
        baseline ``check()`` whose reports both record it and disagree on
        ``"patterns"`` leaves the validity rule, and the overall rule when
        validity is assessed on one side only, ``setting_changed`` on
        ``metric_packs``. Absent from a report written before 0.14, which
        did not record it.

        ``temporal_pairs`` lists the start/end pairs ``temporal_pairs=``
        declared, as ``{"start": ..., "end": ...}``, sorted, each once:
        timeliness compares each pair once whatever order the list gives,
        but a reversed pair counts the opposite rows. Empty when none were
        declared. :meth:`compare` lists it under ``configuration``, and a
        baseline ``check()`` decides a timeliness or overall drop only when
        both reports declare the same pairs. Absent from a report written
        before 0.14, which recorded only the columns the pairs named.

        ``None`` for a report loaded from a document written before dataprof
        recorded them: the settings are unknown, not the defaults.
        """
        settings = self._report.analysis_settings
        return None if settings is None else dict(settings)

    @property
    def generator(self) -> dict[str, _Any] | None:
        """The dataprof build that produced this report.

        ``version`` is the package version. A development build carries the
        version of the release it follows, so where the build recorded them,
        ``commit`` is the full git hash it was compiled from and ``dirty``
        whether the compiled Rust sources had uncommitted changes. Both are
        absent from a build that did not record them, such as one from an
        sdist. :func:`dataprof.capabilities` reports the same for the
        installed build.

        ``None`` for a report loaded from a document written before dataprof
        recorded it. A stored report keeps the value it was written with.
        """
        generator = self._report.generator
        return None if generator is None else dict(generator)

    @property
    def source_identity(self) -> dict[str, _Any] | None:
        """Which data this report describes, beyond where it was read from.

        ``version`` is the version of the source the caller passed as
        ``source_version=`` to :func:`dataprof.profile` (or
        ``--source-version`` to ``python -m dataprof.check``), recorded
        verbatim. A later release may add other kinds of identity under their
        own keys.

        ``None`` when nothing was recorded. A stored report keeps the value it
        was written with.
        """
        identity = self._report.source_identity
        return None if identity is None else dict(identity)

    @property
    def csv_metadata(self) -> dict[str, _Any] | None:
        """How a CSV source was split into fields.

        ``delimiter`` is the delimiter the reader used. ``delimiter_detection``
        is present when detection chose it, not ``csv_delimiter=``: the
        ``delimiter`` it chose, ``header_fields`` (the header's field count),
        and, when the choice was not clear, ``runner_up`` (another candidate
        that read the sample as well) or ``unlisted_delimiter`` (a character
        outside the candidates that splits the header and every sampled
        record alike). ``findings()`` warns about both.

        ``None`` for a source that is not CSV, and for a report loaded from a
        document written before dataprof recorded it.
        """
        metadata = self._report.csv_metadata
        if metadata is None:
            return None
        copied = dict(metadata)
        if "delimiter_detection" in copied:
            copied["delimiter_detection"] = dict(copied["delimiter_detection"])
        return copied

    def _record_source_version(self, source_version: str | None) -> ProfileReport:
        """Record ``source_version`` on a report this process just profiled."""
        if source_version is not None:
            # Every caller wraps a native report it has just built; a loaded
            # report keeps the identity it was written with and never gets here.
            _cast(_RustProfileReport, self._native_report)._record_source_version(source_version)
        return self

    @property
    def semantic_hint_bindings(self) -> list[dict[str, _Any]]:
        """Per-column evidence of how each semantic hint bound to the data.

        Empty unless ``positive_columns``/``identifier_columns``/
        ``temporal_columns``/``temporal_pairs`` were supplied. Each entry has
        ``column``, ``kind``, ``checked_values``, ``matched_values``, and
        ``exact``. A hint proven inert over the full data raises before a report
        is returned, so a report only ever carries bindings that matched
        something or whose evidence was sampled.
        """
        return list(self._report.semantic_hint_bindings)

    @property
    def low_sample_warning(self) -> bool:
        """True when the sample used was below the recommended minimum (10 rows).

        When set, treat ``quality_score`` and the per-dimension ratios as
        directional rather than reliable.
        """
        q = self._report.quality
        return bool(q is not None and q.low_sample_warning)

    @property
    def execution_time_ms(self) -> int:
        return self._report.scan_time_ms

    @property
    def throughput(self) -> float | None:
        return _r4(self._report.throughput_rows_sec)

    @property
    def memory_peak_mb(self) -> float | None:
        return _r2(self._report.memory_peak_mb)

    @property
    def truncation_reason(self) -> str | None:
        return self._report.truncation_reason

    @property
    def source_exhausted(self) -> bool:
        return self._report.source_exhausted

    @property
    def error_count(self) -> int:
        """Number of records skipped because they could not become rows.

        Nonzero means the profile is partial: for tolerant JSON/JSONL parsing
        (``jsonl_on_error="skip"``, the default) each record that is malformed,
        or that is valid JSON but not an object, is skipped and counted here
        rather than aborting the run. Zero means every record became a row.
        """
        return self._report.error_count

    @property
    def ragged_row_count(self) -> int:
        """Number of data rows whose field count differed from the header.

        Nonzero means the source did not parse cleanly: flexible parsing
        recovered each row (dropping extra fields or padding missing ones to
        null), but the row is a structural violation. This is the direct answer
        to "did parsing silently go wrong?" for ragged CSV.
        """
        return self._report.ragged_row_count

    @property
    def unterminated_quote(self) -> bool | None:
        """Whether a CSV source ended inside a quoted field.

        ``True`` means a quote was opened and never closed, so the last record
        read may hold every source row after it in one field. ``None`` means it
        was not checked: non-CSV input, a scan that stopped before the end of
        the source, or a report written before 0.12.
        """
        return self._report.unterminated_quote

    @property
    def _schema_version(self) -> int:
        """The schema version the report was written with; 0 before 0.10.

        Private: it lets a reader tell a field the writer never recorded from
        one it recorded as zero.
        """
        return self._report.schema_version

    @property
    def _ragged_row_count_recorded(self) -> bool:
        """Whether ``ragged_row_count`` is a count the writer recorded.

        Private, like ``_schema_version``: a document from before 0.10 did not
        count ragged rows, and a summary that omits the field records none
        either. Both read back as 0 through the public accessor.
        """
        if self._schema_version == 0:
            return False
        if self._native_report is not None:
            return True
        return bool(self._report._accessor.get("ragged_row_count_recorded"))

    @property
    def sampling_applied(self) -> bool:
        return self._report.sampling_applied

    @property
    def sampling_ratio(self) -> float | None:
        return _r4(self._report.sampling_ratio)

    @property
    def sampled_row_ranges(self) -> list[list[int]] | None:
        """Exact zero-based, half-open source row ranges, when recorded.

        ``None`` means no selection was recorded; an empty list means zero
        rows were selected. Capped Parquet profiles record their spread sample.
        """
        ranges = self._report.sampled_row_ranges
        return None if ranges is None else [list(bounds) for bounds in ranges]

    # -- Mapping protocol --

    def __getitem__(self, key: str) -> ColumnProfile:
        if not isinstance(key, str):
            raise TypeError("ProfileReport keys must be strings")
        profiles = self.column_profiles
        if key not in profiles:
            raise KeyError(key)
        return profiles[key]

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        return key in self.column_profiles

    def __iter__(self) -> _Iterator[str]:
        return iter(self.column_profiles)

    def __len__(self) -> int:
        return self.columns

    # -- Export methods --

    def to_dict(self) -> dict:
        """Convert the report to a nested Python dict.

        All floating-point values are rounded: 2dp for ``0..100`` percentages,
        4dp for statistics and for ``0..1`` ratios such as ``uniqueness_ratio``,
        and moment statistics at most 12 significant digits.
        This is the historical flat summary, produced by the Rust runtime.
        Use :meth:`to_json` or JSON :meth:`save` for the complete persisted
        document. The summary carries ``schema_version``; see
        :meth:`from_dict` for the compatibility policy.
        """
        if self._native_report is not None:
            return _json.loads(self._native_report.summary_json())
        # Legacy flat documents lack full source and quality provenance, so
        # they never become a canonical runtime report. Rebuild the summary
        # from the accessors rather than echoing the input: the export must
        # agree with what the loader kept, dropped or normalized.
        return self._legacy_summary()

    def _legacy_summary(self) -> dict[str, _Any]:
        cols = [column_to_dict(col) for col in self._report.column_profiles]

        quality_dict = None
        q = self._report.quality
        if q is not None:
            quality_dict = {
                "overall_score": _r2(q.overall_quality_score()),
                "assessed_dimensions": q.assessed_dimensions(),
                "dimension_scores": {
                    name: _r2(score) for name, score in q.dimension_scores().items()
                },
                "low_sample_warning": bool(q.low_sample_warning),
            }
            # Absent before 0.14; kept absent so the summary reader keeps
            # reading "not sampled" as exact there (#1027).
            exact = self._report.quality_exact_dimensions
            if exact is not None:
                quality_dict["exact_dimensions"] = list(exact)
            # Absence is preserved: a document from a release that did not
            # record this must not round-trip as "nothing was sampled".
            sampled = self.quality_sampled_dimensions
            if sampled is not None:
                quality_dict["sampled_dimensions"] = sampled
            bounds = self.quality_score_bounds
            if bounds is not None:
                quality_dict["score_bounds"] = bounds
            skipped = self.quality_columns_without_value_checks
            if skipped is not None:
                quality_dict["columns_without_value_checks"] = skipped
            # The Rust summary always names the weights behind its score
            # (#749). A summary that did not record them keeps them absent:
            # the defaults the loader filled in would be a guess.
            if q._accessor.get("score_weights_recorded"):
                quality_dict["score_weights"] = dict(q.score_weights)
            for dimension in _QUALITY_DIMENSIONS:
                values = getattr(q, dimension)
                if values is not None:
                    quality_dict[dimension] = _round_dimension(values)

        document: dict[str, _Any] = {
            "schema_version": REPORT_SCHEMA_VERSION,
            "source": self._report.source,
            "source_type": self._report.source_type,
            **(
                {"source_format": self._report.source_format}
                if self._report.source_format is not None
                else {}
            ),
            "execution": {
                "engine": self._report.engine,
                "rows_processed": self._report.rows_processed,
                "columns_detected": self._report.columns_detected,
                "scan_time_ms": self._report.scan_time_ms,
                "source_exhausted": self._report.source_exhausted,
                "truncation_reason": self._report.truncation_reason,
                "bytes_consumed": self._report.bytes_consumed,
                "throughput_rows_sec": _r4(self._report.throughput_rows_sec),
                "memory_peak_mb": _r2(self._report.memory_peak_mb),
                "error_count": self._report.error_count,
                # Omitted when the loaded document did not record it, so a
                # saved and reloaded summary still reads it as unrecorded,
                # never as a measured zero (#1306 review).
                **(
                    {"ragged_row_count": self._report.ragged_row_count}
                    if self._ragged_row_count_recorded
                    else {}
                ),
                # Omitted when the loaded document did not record it, so the
                # gate reads the coverage as unrecorded, never as unsampled
                # (#1236).
                **(
                    {"sampling_applied": self._report.sampling_applied}
                    if self._report._accessor.get("sampling_applied_recorded")
                    else {}
                ),
                "sampling_ratio": _r4(self._report.sampling_ratio),
            },
            "columns": cols,
            "quality": quality_dict,
            "quality_status": _quality_status_document(self.quality_status, self.quality_error),
        }
        # Additive provenance is omitted when the document did not record it.
        events = self.recovery_events
        if events is not None:
            _cast(dict[str, _Any], document["execution"])["recovery_events"] = events
        ranges = self.sampled_row_ranges
        if ranges is not None:
            _cast(dict[str, _Any], document["execution"])["sampled_row_ranges"] = ranges
        unterminated_quote = self.unterminated_quote
        if unterminated_quote is not None:
            _cast(dict[str, _Any], document["execution"])["unterminated_quote"] = unterminated_quote
        bindings = self.semantic_hint_bindings
        if bindings:
            document["semantic_hint_bindings"] = bindings
        semantics = self.metric_semantics
        if semantics is not None:
            document["metric_semantics"] = semantics
        settings = self.analysis_settings
        if settings is not None:
            document["analysis_settings"] = settings
        generator = self.generator
        if generator is not None:
            document["generator"] = generator
        identity = self.source_identity
        if identity is not None:
            document["source_identity"] = identity
        csv_metadata = self.csv_metadata
        if csv_metadata is not None:
            document["csv_metadata"] = csv_metadata
        return document

    def to_json(self, indent: int = 2) -> str:
        """Export the canonical Rust report document for lossless persistence.

        Unlike the convenience summary returned by :meth:`to_dict`, this keeps
        report identity, detailed source metadata and quality confidence.
        Loaded legacy flat documents retain their original document shape.
        """
        if self._native_report is not None:
            text = self._native_report.to_json()
            if indent == 2:
                return text
            return _json.dumps(_json.loads(text), indent=indent, ensure_ascii=False)
        return _json.dumps(self.to_dict(), indent=indent)

    def _records(self) -> list[dict[str, _Any]]:
        """Build enriched, rounded records for all columns."""
        return [_column_record(col) for col in self._report.column_profiles]

    def to_dataframe(self):
        """Convert column profiles to a pandas DataFrame.

        Includes all available statistics (numeric, text, pattern) with
        proper rounding. Requires pandas.
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for to_dataframe(). Install it with: uv pip install pandas"
            ) from None

        return pd.DataFrame(self._records())

    def to_polars(self):
        """Convert column profiles to a polars DataFrame.

        Same enriched columns as to_dataframe(). Requires polars.
        """
        try:
            import polars as pl
        except ImportError:
            raise ImportError(
                "polars is required for to_polars(). Install it with: uv pip install polars"
            ) from None
        return pl.DataFrame(self._records())

    def to_arrow(self):
        """Convert column profiles to a PyArrow Table.

        Same enriched columns as to_dataframe(). Requires pyarrow.
        """
        try:
            import pyarrow as pa
        except ImportError:
            raise ImportError(
                "pyarrow is required for to_arrow(). Install it with: uv pip install pyarrow"
            ) from None
        return pa.Table.from_pylist(self._records())

    def describe(self) -> _Any:
        """Transposed statistical summary, similar to pandas DataFrame.describe().

        Rows are stats (count, null%, unique, mean, std, min, 25%, 50%, 75%,
        max, min_length, max_length, avg_length). Columns are dataset columns.
        Returns a pandas DataFrame if pandas is available, otherwise a
        dict-of-dicts.
        """
        summary: dict[str, dict[str, _Any]] = {}
        for col in self._report.column_profiles:
            q = _round_quartiles(col.quartiles)
            d: dict[str, _Any] = {
                "count": col.total_count,
                "null%": _r2(col.null_percentage),
                "unique": col.unique_count,
                "mean": _rstat(col.mean),
                "std": _rstat(col.std_dev),
                "min": _r4(col.min),
                "25%": q["q1"] if q else None,
                # Small samples compute a median without full quartiles;
                # fall back so 50% matches the median shown elsewhere.
                "50%": q["q2"] if q else _r2(col.median),
                "75%": q["q3"] if q else None,
                "max": _r4(col.max),
                "min_length": col.min_length,
                "max_length": col.max_length,
                "avg_length": _r4(col.avg_length),
                "true_count": col.true_count,
                "false_count": col.false_count,
                "true_ratio": _r4(col.true_ratio),
            }
            summary[col.name] = d

        try:
            import pandas as pd

            return pd.DataFrame(summary)
        except ImportError:
            return summary

    def check(
        self,
        *,
        min_rows: int | None = None,
        min_quality_score: float | None = None,
        min_dimension_scores: _Mapping[str, float] | None = None,
        max_null_percentage: _Mapping[str, float] | float | None = None,
        max_null_count: _Mapping[str, int] | int | None = None,
        max_duplicate_rows: int | None = None,
        max_date_age: _Mapping[str, int] | None = None,
        max_date_gap: _Mapping[str, int] | None = None,
        require_metrics: _Sequence[str] | None = None,
        scope: str = "full_source",
        baseline: ProfileReport | None = None,
        max_quality_score_drop: float | None = None,
        max_dimension_score_drops: _Mapping[str, float] | None = None,
        max_null_percentage_increase: _Mapping[str, float] | float | None = None,
        max_distinct_drop: _Mapping[str, float] | float | None = None,
        max_row_count_drop: float | None = None,
        max_row_count_growth: float | None = None,
        require_columns: _Sequence[str] | None = None,
        forbid_removed_columns: bool = False,
        forbid_added_columns: bool = False,
        forbid_findings: _Sequence[str] | None = None,
        timeliness_clock: str = "fixed",
    ) -> _QualityGateResult:
        """Evaluate a quality policy against this report.

        Returns a :class:`~dataprof._gate.QualityGateResult`: structured,
        deterministic data. Nothing is printed, the process is not exited, and
        the report is not modified, so this composes into any orchestrator::

            result = report.check(
                min_quality_score=90,
                max_null_count={"customer_id": 0},
                max_null_percentage={"*": 20},
                require_metrics=["quality"],
            )
            if not result.passed:
                for violation in result.violations:
                    print(violation.code, violation.message)

        The verdict is three-valued. ``"fail"`` means a requirement was
        conclusively violated; ``"inconclusive"`` means nothing was violated
        and something could not be checked; ``"pass"`` means everything was
        checked and met. ``result.passed`` is True only for ``"pass"``, so an
        unanswerable gate never reads as a green one.

        Args:
            min_rows: Floor for the number of rows the scan read, so an empty
                or truncated extract fails instead of passing every other
                check. A scan that read this many rows passes even when it
                stopped early; one that read fewer fails, unless it stopped
                early under ``"full_source"`` scope, which leaves the check
                unevaluated.
            min_quality_score: Floor for the overall score, 0-100.
            min_dimension_scores: Floor per ISO 25012 dimension, 0-100.
            max_null_percentage: Ceiling for a column's null percentage,
                0-100. A mapping keyed by column name, where ``"*"`` sets the
                limit for every column without its own entry; a bare number is
                the same as ``{"*": number}``. Decided at the two decimals it
                is shown at, so above 20,000 rows a zero limit lets one null
                through; use ``max_null_count`` for "no nulls".
            max_null_count: Ceiling for a column's null count, decided on the
                exact count. Same mapping shape as ``max_null_percentage``.
                ``{"customer_id": 0}`` states "no nulls" at any row count.
            max_duplicate_rows: Ceiling for the duplicate-row count.
            max_date_age: Days a column's newest date (its ``max_datetime``)
                may lie before the timeliness reference date the report
                recorded, so a feed that stopped moving fails:
                ``{"published_at": 3}``. The reference date is the profiling
                call's ``reference_date``, or the run's date when none was
                given, and the age is counted in calendar days. A date after
                the reference date is fresh. A newest date read on part of a
                source can only be overtaken by a newer one, so under
                ``"full_source"`` a fresh date passes on any scan, and a stale
                one fails only when every row was read and the column's date
                statistics saw every value; otherwise it is
                ``evidence_incomplete``. Slash dates are the exception: their
                day and month order is decided from the values read, so a
                fresh one read on part of a column is ``evidence_incomplete``
                while the unread values could still change that order. A
                column with no newest date (not typed as a date, all null) is
                ``not_assessed``.
            max_date_gap: Days the largest gap between a column's
                consecutive distinct dates may span, so a daily file that lost
                a period from the middle of its range fails:
                ``{"published_at": 1}`` allows no missing day. The gap is the
                column's ``largest_gap``, read over every value profiled.
                Unread rows can fill a gap or open a new one at either edge,
                so under ``"full_source"`` the rule decides only when every
                row was read and the column's date statistics saw every
                value; otherwise it is ``evidence_incomplete`` either way. A
                column with no gap recorded (not typed as a date, all null, a
                report from before 0.14) is ``not_assessed``.
            require_metrics: Metrics that must have been analyzed at all:
                ``"quality"`` for the assessment as a whole, or a dimension
                name. Without this, a report with no assessment leaves the
                related checks unevaluated rather than failed.
            scope: ``"full_source"`` (the default) states the requirements
                about the entire source, so a truncated or sampled scan leaves
                them unevaluated unless it already witnessed a violation.
                ``"observed"`` states them about whatever the metrics actually
                measured, which is always evaluable and says nothing about the
                rows that were not read.
            baseline: A saved report to measure change from, such as
                yesterday's run. The rules about change below need one; the
                other rules ignore it. It may be loaded from a flat
                ``to_dict()`` summary, which leaves what it did not record
                unrecorded.
            max_quality_score_drop: How many points the overall score may
                fall below the baseline's, 0-100. Under ``"full_source"``, a
                score computed over a retained sample is read as its
                ``quality_score_bounds`` interval on either side.
            max_dimension_score_drops: The same per ISO 25012 dimension.
            max_null_percentage_increase: How many points a column's null
                percentage may rise above the baseline's, 0-100. A mapping
                keyed by column name, where ``"*"`` sets the limit for every
                other column both reports profiled; a bare number is the same
                as ``{"*": number}``. A column only this report has is checked
                only when named.
            max_distinct_drop: How far a column's distinct count may fall,
                in percent (0-100) of the baseline's count, as a key that
                suddenly repeats. Same mapping shape as
                ``max_null_percentage_increase``. Fewer rows alone are not a
                collapse: the check fails only when the column's distinct
                share of non-null values also fell by more than that
                percentage of the baseline's share. Decided on exact counts
                only, so a count either side estimated, or did not record as
                exact, leaves it ``approximate_count``.
            max_row_count_drop: How far the row count may fall, in percent
                (0-100) of the baseline's ``rows_processed``, as a truncated
                extract. A scan that stopped early read a lower bound of its
                source's rows, so under ``"full_source"`` an incomplete scan
                of this report still decides a pass, an incomplete baseline
                still decides a failure, and the other outcome is
                ``evidence_incomplete``. An allowance of 100 passes whatever
                the scans missed, since a count cannot fall by more. A
                different column set leaves it ``schema_changed``.
            max_row_count_growth: How far the row count may grow, in percent
                of the baseline's, 0 or more, as a join that fans out or a
                file published twice. The mirror of ``max_row_count_drop``:
                an incomplete scan of this report still decides a failure,
                and an incomplete baseline a pass.
            require_columns: Columns this report must have; needs no baseline.
                One check per name, in name order.
            forbid_removed_columns: Require every column the baseline profiled
                to be profiled still. One check per baseline column.
            forbid_added_columns: Allow no column the baseline did not profile:
                a closed schema. One check per column of this report.
            forbid_findings: Codes of :meth:`findings`, such as
                ``"locale_numbers"``: the report must hold no finding with
                one, derived at the :meth:`findings` default thresholds.
                One ``forbid_finding`` check per code, in code order,
                carrying the findings it found. Under ``"full_source"``, a
                finding about how the source was read (``partial_scan``,
                ``delimiter_ambiguous``, ``header_not_split``) is decided on
                any scan; one rows witness (``duplicate_rows``,
                ``key_duplicates``, ``future_dates``,
                ``temporal_order_violations``, ``ragged_rows``,
                ``records_skipped``, ``unterminated_quote``) fails a partial
                scan but passes only a complete one; one about a share of
                a column's values (``null_heavy``, ``all_null``,
                ``constant_column``, ``mixed_types``, ``locale_numbers``,
                ``sensitive_pattern``) is decided only on a complete scan.
                A rule :meth:`findings` lists in ``not_evaluated`` that
                found nothing leaves the check ``finding_not_evaluated``,
                with its entries under ``rules``. Needs no baseline.
            timeliness_clock: How the timeliness clocks of the baseline and
                this report may differ, for the overall and timeliness score
                drop rules. ``"fixed"`` (the default) needs the same
                reference date on both sides, as when every run passes the
                same ``reference_date``. ``"rolling"`` lets two rolling
                clocks, each read on its own run day, have different dates,
                and compares the scores as measured. Under either, the time
                zone and the maximum age must match, and a clock either side
                did not record leaves the rule ``setting_unrecorded`` on the
                setting ``timeliness_clock``.

        A missing column fails a column rule only when the side without it
        shows its source lacks it: it recorded its column selection, the
        selection included the column, and its column set covers the whole
        source. A CSV header, a Parquet schema, a DataFrame or a query result
        lists every column however few rows were read; for JSON, JSONL and
        streams only a scan of every record does. Otherwise the check is
        unevaluated: ``setting_changed`` or ``setting_unrecorded`` on the
        setting ``columns``, ``column_not_profiled`` for a required column the
        run did not select, or ``evidence_incomplete``.

        A rule about change is decided only when both reports measured the
        same thing the same way. Otherwise it is unevaluated, with the reason
        in the check: ``not_in_baseline``; ``schema_changed`` for a score
        when the column sets differ (a column's own rule still decides);
        ``definition_not_comparable`` for a ``metric_semantics`` key the rule
        depends on; ``setting_changed`` or ``setting_unrecorded`` for a
        setting that moves the score on unchanged data, such as a semantic
        hint; or, under ``"full_source"``, ``evidence_incomplete`` when either
        side does not cover its whole source. A score drop rule reads a
        score computed over a retained sample of a fully read source as its
        recorded interval, and is decided when every change the two sides'
        intervals allow lies on one side of the allowance; the check records
        them as ``bounds`` and ``baseline["bounds"]``. ``result.baseline`` names both
        reports and how their columns differ, and its ``diagnostics`` list
        each column added, removed, retyped or now all null, which never move
        the verdict.

        ``result.generator`` and ``result.source_identity`` name the build
        that wrote this report and the caller's version of its source, each
        ``None`` when the report did not record it.

        Raises:
            ValueError: the policy is unevaluable as written -- a threshold
                outside 0-100, an unknown dimension name, a rule about change
                without a baseline, or no requirement at all. A misconfigured
                gate is not a failing dataset. Also raised for a report loaded
                from a flat ``to_dict()`` summary, which the Rust gate cannot
                read; the baseline may be one. An unknown findings code in
                ``forbid_findings``.
            TypeError: ``baseline`` is not a :class:`ProfileReport`.
        """
        if baseline is not None and not isinstance(baseline, ProfileReport):
            raise TypeError(
                f"baseline must be a dataprof ProfileReport, got {type(baseline).__name__}"
            )
        try:
            policy = _Policy(
                min_rows=min_rows,
                min_quality_score=min_quality_score,
                min_dimension_scores=min_dimension_scores,
                max_null_percentage=max_null_percentage,
                max_null_count=max_null_count,
                max_duplicate_rows=max_duplicate_rows,
                max_date_age=max_date_age,
                max_date_gap=max_date_gap,
                require_metrics=require_metrics,
                scope=scope,
                baseline=None if baseline is None else baseline._delta_input(),
                max_quality_score_drop=max_quality_score_drop,
                max_dimension_score_drops=max_dimension_score_drops,
                max_null_percentage_increase=max_null_percentage_increase,
                max_distinct_drop=max_distinct_drop,
                max_row_count_drop=max_row_count_drop,
                max_row_count_growth=max_row_count_growth,
                require_columns=require_columns,
                forbid_removed_columns=forbid_removed_columns,
                forbid_added_columns=forbid_added_columns,
                forbid_findings=forbid_findings,
                timeliness_clock=timeliness_clock,
            )
        except _PolicyFieldError as exc:
            if exc.key != "baseline":
                raise
            raise ValueError(f"{exc}: pass baseline=ProfileReport.load('baseline.json')") from None
        return policy.evaluate(self._native_report)

    def findings(
        self,
        *,
        null_heavy_percentage: float | None = None,
        mixed_types_percentage: float | None = None,
    ) -> _FindingsResult:
        """Derive structured, prioritized findings from this report.

        Answers "what deserves attention?" without inventing thresholds at the
        call site::

            for finding in report.findings():
                print(finding.severity, finding.code, finding.column, finding.evidence)

        Each finding carries a stable ``code``, a ``severity`` (``"warning"``
        or ``"info"``), the ``column`` it concerns when it concerns one, the
        ``evidence`` that caused it, and a fixed ``summary`` sentence. No
        finding carries a raw cell value. Findings are interpretation, not
        cleaning advice, and they are derived on demand rather than stored, so
        a loaded report yields the same findings as the one that was saved.

        A rule whose input the report does not carry, such as patterns that
        were not detected or a quality assessment that was not requested,
        produces no finding and is listed in ``result.not_evaluated`` with the
        reason. An empty ``result.findings`` is a clean result only for the
        rules not listed there, so ``bool(result)`` raises rather than
        answering.

        Findings describe the rows the report read; a truncated or sampled
        scan adds a ``partial_scan`` finding instead of withholding the rest.
        For a pass/fail decision about the whole source, use :meth:`check`.

        Args:
            null_heavy_percentage: Report a column as ``null_heavy`` when its
                null percentage is at least this, 0-100 exclusive of 0.
                Defaults to 20, the threshold ``to_llm_context()`` flags at.
            mixed_types_percentage: Report a column as ``mixed_types`` when the
                values outside its dominant lexical type are at least this
                share of those classified, 0-100 exclusive of 0. Defaults to 5.

        Raises:
            ValueError: a threshold is not a percentage above 0 and at most 100.
        """
        return _evaluate_findings(
            self,
            null_heavy_percentage=null_heavy_percentage,
            mixed_types_percentage=mixed_types_percentage,
        )

    def quality_summary(self) -> dict[str, _Any]:
        """Single-row quality summary for easy aggregation.

        Returns a dict with source, rows, quality_score, per-dimension scores,
        and execution_time_ms. Useful for pd.concat() across multiple reports.

        Dimension scores mirror the components of ``quality_score``: each uses
        every sub-metric of its dimension, and a dimension that had nothing to
        assess (no numeric values, no date columns, ...) is None rather than a
        vacuous 100.
        """
        q = self._report.quality
        row: dict[str, _Any] = {
            "source": self._report.source,
            "rows": self._report.rows_processed,
            "quality_score": _r2(self._report.quality_score),
            "completeness": None,
            "consistency": None,
            "uniqueness": None,
            "accuracy": None,
            "timeliness": None,
            "validity": None,
            "precision": None,
            "execution_time_ms": self._report.scan_time_ms,
        }
        if q is not None:
            for name, score in q.dimension_scores().items():
                row[name] = _r2(score)
        return row

    def save(self, path: str | _os.PathLike[str]) -> ProfileReport:
        """Save the report to a file.

        Supported formats (by extension):
            .json        — full report as JSON
            .csv         — column profiles as CSV (no extra dependencies)
            .parquet     — column profiles as Parquet (requires pyarrow)
            .html        — standalone HTML render (same as :meth:`to_html`)
            .md/.markdown — markdown table (same as :meth:`to_markdown`)

        Args:
            path: File path with one of the supported extensions.

        Returns:
            ``self`` for fluent chaining.
        """
        path = _normalize_pathlike(path)
        lower_path = path.lower()

        if lower_path.endswith(".json"):
            _atomic_write_text(path, self.to_json(), newline="\n")

        elif lower_path.endswith(".csv"):
            records = self._records()

            # Opening unconditionally matters for a zero-column report: save()
            # must either create the requested artifact or fail, never return
            # success while leaving no file behind.
            def write_csv(temp_path: str) -> None:
                with open(temp_path, "w", encoding="utf-8", newline="") as f:
                    if records:
                        writer = _csv.DictWriter(f, fieldnames=records[0].keys())
                        writer.writeheader()
                        writer.writerows(records)

            _atomic_write(path, write_csv)
        elif lower_path.endswith(".parquet"):
            table = self.to_arrow()
            import pyarrow.parquet as pq

            def write_parquet(temp_path: str) -> None:
                pq.write_table(table, temp_path)

            _atomic_write(path, write_parquet)

        elif lower_path.endswith(".html"):
            _atomic_write_text(path, self.to_html())

        elif lower_path.endswith((".md", ".markdown")):
            _atomic_write_text(path, self.to_markdown())
        else:
            raise ValueError(
                f"Unsupported format for '{path}'. "
                "Use .json, .csv, .parquet, .html, .md, or .markdown."
            )
        return self

    def to_html(self) -> str:
        """Return the embeddable HTML representation of the report.

        Identical to Jupyter's rich display (``_repr_html_``), exposed as a
        public method for saving an HTML fragment, embedding in CI summaries,
        or sharing outside a notebook.
        """
        return self._repr_html_()

    def to_markdown(self) -> str:
        """Render the report as a GitHub-flavored markdown table.

        Suitable for issue bodies, pull-request comments, Slack posts, or
        README snippets. Uses the same per-column summary as the HTML view.
        """

        def esc(value: object) -> str:
            return _one_line(value).replace("|", "\\|")

        qs = self.quality_score
        qs_str = f"{qs:.1f}%" if qs is not None else "N/A"
        lines = [
            f"**Source:** {esc(self.source)} | **Rows:** {self.rows:,} | "
            f"**Columns:** {self.columns} | **Quality:** {qs_str} | "
            f"**Time:** {self.execution_time_ms}ms",
            *([f"**Note:** {esc(note)}"] if (note := self._value_checks_note()) else []),
            "",
            "| Column | Type | Count | Null % | Unique | Stats | Pattern |",
            "|---|---|---|---|---|---|---|",
        ]
        for col in self._report.column_profiles:
            if col.unique_count is None:
                unique_str = ""
            else:
                # Unknown provenance (None) must not look exact -- only an
                # explicit False drops the ~ prefix.
                prefix = "" if col.unique_count_is_approximate is False else "~"
                unique_str = f"{prefix}{col.unique_count:,}"
            row = [
                esc(col.name),
                esc(col.data_type),
                f"{col.total_count:,}",
                _pct_str(col.null_percentage),
                unique_str,
                esc(_stats_cell(col)),
                esc(_pattern_cell(col)),
            ]
            lines.append("| " + " | ".join(row) + " |")
        return "\n".join(lines)

    def to_llm_context(self, max_tokens: int = 1000, include_samples: bool = False) -> str:
        """Render a token-bounded, agent-oriented summary of the report.

        Answers "what is this dataset and what is wrong with it?" in a form an
        LLM can consume cheaply::

            report = dp.profile("orders.csv")
            print(report.to_llm_context())

        Emits structured signals only -- shape, provenance caveats, quality
        flags, schema, and detected pattern names. Raw cell values are redacted
        by default, and detected sensitive-pattern values are never echoed. No
        recommendations and no generative prose; interpretation is the caller's
        job.

        :param max_tokens: Approximate upper bound on the output, enforced by
            deterministic truncation with a ``... +N more`` tail. Tokens are
            estimated as ``ceil(len(text) / 4)``, not counted with a real
            tokenizer, so treat this as a budget rather than an exact size. The
            dataset header is always emitted even if it exceeds the budget.
        :param include_samples: When ``True``, include non-sensitive numeric
            extrema (column minima and maxima). Off by default because these
            are raw cell values. Extrema are emitted only for columns that
            pattern detection scanned and cleared. A column omits them when a
            sensitive pattern was detected, and *every* column omits them when
            the report carries no pattern evidence at all -- profiled without
            the ``"patterns"`` metric pack, or rebuilt from a payload whose
            ``patterns`` key was absent. Evidence survives ``save()``/``load()``,
            so a round-tripped report redacts exactly as the original did.
        :returns: A plain-text summary. Stable across runs for a given report.
        """
        return self._to_llm_context(
            source=self.source, max_tokens=max_tokens, include_samples=include_samples
        )

    def _to_llm_context(self, *, source: str, max_tokens: int, include_samples: bool) -> str:
        """Render with a caller-selected source label before budgeting the header."""
        qs = self.quality_score
        qs_str = f"{qs:.1f}/100" if qs is not None else "n/a"

        header = [
            f"dataset: {_one_line(source)} ({self.source_type})",
            f"rows: {self.rows:,} | columns: {self.columns} | quality: {qs_str}",
        ]

        # Provenance caveats change how every number below should be read, so
        # they ride with the header rather than competing for section budget.
        if self.sampling_applied:
            ratio = self.sampling_ratio
            suffix = f" (ratio {ratio:.4g})" if ratio is not None else ""
            header.append(f"caveat: profile is based on a sample{suffix}")
        if self.truncation_reason:
            header.append(f"caveat: scan stopped early ({self.truncation_reason})")
        if self.low_sample_warning:
            header.append("caveat: low sample size, quality metrics are unreliable")
        note = self._value_checks_note()
        if note is not None:
            header.append(f"caveat: {note}")
        # "quality: n/a" reads as a skipped run whatever the reason was. A
        # computation that was asked for and broke is a caveat about the whole
        # report, not a missing number.
        #
        # The message comes from a loaded document, so it is untrusted text in a
        # line-oriented context: a newline in it would forge caveat lines of the
        # reader's own format. `_one_line` is what every other borrowed string
        # here goes through.
        if self.quality_status != "computed":
            error = self.quality_error
            detail = f": {_bounded(_one_line(error))}" if error else ""
            header.append(
                f"caveat: no quality assessment ({_one_line(self.quality_status)}{detail})"
            )

        header_text = "\n".join(header)
        budget = max_tokens - _estimate_tokens(header_text)
        if budget <= 0:
            return header_text

        cols = list(self._report.column_profiles)

        flag_items = [
            f"- {text}"
            for _, text in sorted(
                (f for col in cols for f in _column_flags(col)),
                key=lambda f: (-f[0], f[1]),  # severity desc, then name for stability
            )
        ]

        schema_items = []
        for col in cols:
            line = f"- {_one_line(col.name)}: {col.data_type}"
            if (
                include_samples
                and _may_expose_values(col)
                and col.min is not None
                and col.max is not None
            ):
                line += f" [{_one_line(col.min)} .. {_one_line(col.max)}]"
            schema_items.append(line)

        pattern_items = []
        for col in cols:
            pattern = _pattern_cell(col)
            if pattern:
                pattern_items.append(f"- {_one_line(col.name)}: {pattern}")

        # Flags are the reason an agent asked; schema is context; patterns are a
        # bonus. Unused budget rolls forward, so a clean dataset spends its flag
        # allowance on schema instead of wasting it.
        sections: list[tuple[str, list[str], float]] = [
            (f"flags ({len(flag_items)}):", flag_items, 0.45),
            (f"schema ({len(schema_items)}):", schema_items, 0.40),
            ("patterns:", pattern_items, 0.15),
        ]

        body: list[str] = []
        remaining = budget
        for i, (title, items, share) in enumerate(sections):
            is_last = i == len(sections) - 1
            # A section's share caps how much it may take, but it may always
            # claim enough for one item -- otherwise a tight budget starves the
            # high-priority sections and spends everything on the last one.
            cap = remaining if is_last else max(0, int(budget * share))
            floor = _section_min_cost(title, items) + 1  # +1 for the separator
            allowance = min(remaining, max(cap, floor))
            lines, used = _fit_section(title, items, max(0, allowance - 1))
            if lines:
                body.extend(["", *lines])
                remaining -= used + 1
            elif items:
                # This section had something to say and no room to say it. Stop:
                # rendering a lower-priority section now would let a reader infer
                # that the omitted one was empty -- e.g. "patterns but no flags"
                # reads as a clean dataset.
                break

        return "\n".join([*header, *body])

    def compare(self, other: ProfileReport) -> dict[str, _Any]:
        """Compare this report (``a``) with ``other`` (``b``) and return the deltas.

        The result captures quality drift and schema differences between two
        profiles, such as a saved baseline and today's run. It is computed by
        the Rust ``ReportDelta``, so Rust and Python return the same document:

        - ``quality_score``: ``a``, ``b``, ``abs`` (``b - a`` in points) and
          ``rel_pct`` (``b - a`` as a percentage of ``a``), plus ``evidence``,
          the coverage behind each side's score.
        - ``dimensions``: the same shape for each ISO 25012 dimension
          (completeness, consistency, uniqueness, accuracy, timeliness,
          validity, precision), as :meth:`quality_summary` reports them.
        - ``columns``: per-column ``null_pct_a``, ``null_pct_b`` and
          ``null_pct_delta`` over the union of column names, this report's
          order first. Each entry also has ``data_type_a`` and
          ``data_type_b``, the inferred type (``"integer"``), ``None`` on a
          side that lacks the column or read no value for it that is not
          null, so two different names are what :meth:`check` reports as
          ``column_type_changed``; and ``native_type_a`` and
          ``native_type_b``, the type the source declared (see
          :attr:`ColumnProfile.native_type`), ``None`` where it declared none.
        - ``schema``: column names ``added`` / ``removed`` / ``common``.
        - ``metric_semantics``: each side's :attr:`metric_semantics` and
          ``comparable``: ``True`` when both record every definition this
          build knows, with the same values, and ``None`` when either side
          does not, which includes every report written before 0.12. Only
          ``True`` means a difference in a measurement, such as a text
          length, is a difference in the data.
        - ``configuration``: the recorded settings that move a score on
          unchanged data, ``score_weights``, ``semantic_hints`` (the
          columns named by ``positive_columns``, ``identifier_columns``,
          ``temporal_columns`` and ``temporal_pairs``), ``pattern_locale``
          (see :attr:`analysis_settings`), ``timeliness_clock`` (the
          ``clock`` in ``quality.timeliness``), ``columns`` (each side's
          ``columns=`` selection, see :attr:`analysis_settings`; ``same``
          ignores the order a list gives), ``metric_packs`` (the packs
          each side computed, see :attr:`analysis_settings`) and
          ``temporal_pairs`` (the pairs each side declared, see
          :attr:`analysis_settings`; ``same`` compares the pairs, not the
          order or repetition of a list). Each has
          ``a``, ``b`` and ``same``: ``True`` when both sides recorded it
          and agree, ``False`` when they differ, ``None`` when either did
          not record it (weights on a summary saved before 0.12, hints
          before 0.10, the locale and the selection before 0.13, the clock,
          the packs and the pairs before 0.14, the clock also with no timeliness
          assessment). Two rolling clocks read on different days differ.
          A score that moved under a different setting may not reflect a change
          in the data.
        - ``reports``: ``a`` and ``b``, each with its ``id`` and
          ``timestamp`` (``None`` for a flat summary), ``source``,
          ``rows_processed``, the scan's ``evidence``, ``generator`` (the
          build that wrote it, see :attr:`generator`) and ``source_identity``
          (see :attr:`source_identity`), each ``None`` where the side did not
          record it; and ``same_build``, which reads the builds in order and
          stops at the first answer: ``None`` when either side recorded no
          build, ``False`` when the versions differ, ``None`` when either
          side recorded no commit, ``False`` when the commits differ,
          ``None`` when either side recorded no dirty flag, then ``True``
          when both are clean, ``False`` when one is dirty, and ``None``
          when both are dirty builds of one commit. A known difference gives
          ``False`` even when a later field is missing. A change
          between two builds can come from a definition that changed between
          them.

        Every change is ``b - a``: a positive score change is an improvement,
        a positive ``null_pct_delta`` is more nulls. Numbers are compared at
        their serialized 2-decimal precision. A number either side lacks
        leaves the change ``None``, and so does ``rel_pct`` when ``a`` is 0.

        Each ``evidence`` is ``{"coverage": "complete"}``, or
        ``{"coverage": "incomplete", "reason": ...}`` with the reason
        ``check()`` uses: ``truncated``, ``sampled``, ``records_skipped``,
        ``quality_sampled`` or ``coverage_unrecorded``. A change between an
        exact and a sampled side can be a sampling difference.

        The shape is stable: later releases may add keys, but do not remove
        or rename them. Either report may be loaded from a flat ``to_dict()``
        summary, including one saved before 0.12.

        Raises:
            TypeError: ``other`` is not a :class:`ProfileReport`.
        """
        if not isinstance(other, ProfileReport):
            raise TypeError(
                f"other must be a dataprof ProfileReport, got {type(other).__name__}; "
                "profile the data first, or load a saved report with ProfileReport.load(path)"
            )
        return _json.loads(_report_delta_json(self._delta_input(), other._delta_input()))

    def _delta_input(self) -> _RustProfileReport | str:
        # A flat summary never becomes a native report (it lacks the
        # provenance one records), so it crosses as the summary it exports.
        if self._native_report is not None:
            return self._native_report
        summary = self._legacy_summary()
        if self._schema_version == 0:
            # The export names the current schema version, but a summary from
            # before 0.10 recorded no hints: without the version the delta
            # reads them as unrecorded rather than as "none given".
            del summary["schema_version"]
        return _json.dumps(summary)

    @classmethod
    def from_dict(cls, data: dict[str, _Any]) -> ProfileReport:
        """Rebuild a read-only report from a canonical document or :meth:`to_dict` summary.

        The reconstructed report uses the same read-only accessors as a live
        report, reading saved values rather than the native engine. All export methods
        (``to_json``, ``to_markdown``, ``to_dataframe``, ``describe``,
        ``quality_summary``, mapping access, …) work as usual. Useful for
        reloading a report saved yesterday without re-profiling the data.

        Compatibility: the document's ``schema_version`` (see
        ``dataprof.REPORT_SCHEMA_VERSION``) is checked first. Documents
        without the field are legacy pre-0.10 reports and load through a
        compatibility path; unknown additive fields from newer writers are
        ignored; a document with a newer schema version is rejected outright
        rather than partially decoded.

        Raises:
            ValueError: if ``data`` is not a mapping produced by ``to_dict()``,
                or was written with a newer report schema than this dataprof
                can read.
        """
        if not isinstance(data, dict):
            raise ValueError(
                "from_dict() expects a mapping produced by ProfileReport.to_dict() "
                "(with 'source', 'columns', and 'execution' keys)."
            )
        # Check the schema version before any structural decoding: an
        # incompatible document must fail explicitly, never load partially.
        # An explicit null is not the same as a missing field: only documents
        # written before versioning existed may omit it.
        if "schema_version" in data:
            version = data["schema_version"]
            if isinstance(version, bool) or not isinstance(version, int) or version < 0:
                raise ValueError(
                    f"from_dict(): 'schema_version' must be an integer, got {version!r}."
                )
            if version > REPORT_SCHEMA_VERSION:
                raise ValueError(
                    f"This report uses schema version {version}, but this dataprof "
                    f"reads up to version {REPORT_SCHEMA_VERSION}. Upgrade dataprof "
                    "to load it."
                )
        # A complete flat signature wins: the v1 additive-field policy lets a
        # summary carry unknown keys, including ones the canonical layout uses.
        flat = {"source", "columns", "execution"} <= data.keys()
        if not flat and ("data_source" in data or "column_profiles" in data):
            return cls(_RustProfileReport.from_json(_json.dumps(data)))
        if not flat:
            raise ValueError(
                "from_dict() expects a mapping produced by ProfileReport.to_dict() "
                "(with 'source', 'columns', and 'execution' keys)."
            )
        if not isinstance(data["execution"], dict):
            raise ValueError("from_dict(): 'execution' must be a mapping.")
        columns = data["columns"]
        if not isinstance(columns, list) or not all(isinstance(c, dict) for c in columns):
            raise ValueError("from_dict(): 'columns' must be a list of mappings.")
        return cls(_report_from_dict(data))

    @classmethod
    def from_json(cls, text: str) -> ProfileReport:
        """Rebuild a read-only ProfileReport from JSON produced by :meth:`to_json`.

        See :meth:`from_dict` for the reconstruction semantics.

        Raises:
            ValueError: if ``text`` is not valid JSON from ``to_json()``.
        """
        try:
            data = _json.loads(text)
        except _json.JSONDecodeError as exc:
            raise ValueError(f"from_json() received invalid JSON: {exc}") from exc
        return cls.from_dict(data)

    @classmethod
    def load(cls, path: str | _os.PathLike[str]) -> ProfileReport:
        """Reload a report previously written with :meth:`save`.

        This is the path-based counterpart to :meth:`from_json` (which takes a
        JSON *string*) and :meth:`from_dict` (which takes a *dict*)::

            before.save("before.json")
            loaded = dp.ProfileReport.load("before.json")

        Only ``.json`` files carry a full report and can be reloaded. The
        ``.csv`` / ``.parquet`` outputs of :meth:`save` store only column
        profiles, not the full report, so they cannot round-trip.

        Args:
            path: Path to a ``.json`` file produced by ``save()``.

        Raises:
            ValueError: if the file is ``.csv`` / ``.parquet`` (profiles only)
                or has an unsupported extension.
            FileNotFoundError: if the file does not exist.
        """
        path = _normalize_pathlike(path)
        lower_path = path.lower()
        if lower_path.endswith(".json"):
            with open(path, encoding="utf-8") as f:
                return cls.from_json(f.read())
        if lower_path.endswith((".csv", ".parquet")):
            raise ValueError(
                f"Cannot reload a full report from '{path}': .csv/.parquet store "
                "only column profiles. Save and load with .json to round-trip a report."
            )
        raise ValueError("Unsupported format. Use .json (produced by save()).")

    def __repr__(self) -> str:
        qs = self.quality_score
        qs_str = f"{qs:.1f}%" if qs is not None else "N/A"
        lines = [
            f"ProfileReport(source='{self.source}', "
            f"rows={self.rows:,}, columns={self.columns}, "
            f"time={self.execution_time_ms}ms, quality={qs_str})"
        ]
        note = self._value_checks_note()
        if note is not None:
            lines.append(f"Note: {note}")

        cols = list(self._report.column_profiles)
        show = cols[: self._MAX_REPR_COLUMNS]
        if show:
            lines.append("Columns:")
        for col in show:
            parts = [f"  {col.name:<20s} {col.data_type:<10s}"]
            parts.append(f"{_pct_str(col.null_percentage):>6} null")
            if col.unique_count is not None:
                # Unknown provenance (None, e.g. an older deserialized report)
                # must not render as exact -- only an explicit False drops the ~.
                approx = "" if col.unique_count_is_approximate is False else "~"
                parts.append(f"{approx}{col.unique_count:,} unique")
            if col.mean is not None:
                parts.append(f"mean={_rstat(col.mean)}")
                if col.std_dev is not None:
                    parts.append(f"std={_rstat(col.std_dev)}")
            elif col.true_count is not None:
                pct = _r2(col.true_ratio * 100) if col.true_ratio is not None else 0
                parts.append(f"true={col.true_count}({pct:.0f}%)")
            elif col.avg_length is not None:
                parts.append(f"avg_len={_r4(col.avg_length)}")
            best = _dominant_pattern(col)
            if best is not None:
                parts.append(f"{best.name}({_r2(best.match_percentage):.0f}%)")
            lines.append("   ".join(parts))

        remaining = len(cols) - len(show)
        if remaining > 0:
            lines.append(f"  ...and {remaining} more columns")
        return "\n".join(lines)

    def _repr_html_(self) -> str:
        """Rich HTML representation for Jupyter notebooks."""
        qs = self.quality_score
        if qs is not None:
            if qs >= 90:
                badge_color = "#22c55e"
            elif qs >= 70:
                badge_color = "#eab308"
            else:
                badge_color = "#ef4444"
            qs_html = (
                f'<span style="background:{badge_color};color:#fff;'
                f'padding:2px 8px;border-radius:4px;font-weight:bold">'
                f"{qs:.1f}%</span>"
            )
        else:
            qs_html = "N/A"
        note = self._value_checks_note()
        note_html = f"<p><i>Note: {_html.escape(note)}</i></p>" if note is not None else ""

        col_rows = ""
        for i, col in enumerate(self._report.column_profiles):
            bg = "#f9fafb" if i % 2 else "#ffffff"
            stats = _stats_cell(col)
            pattern = _html.escape(_pattern_cell(col))
            if col.unique_count is None:
                unique_str = ""
            else:
                # Unknown provenance (None) must not look exact -- only an
                # explicit False drops the ~ prefix.
                prefix = "" if col.unique_count_is_approximate is False else "~"
                unique_str = f"{prefix}{col.unique_count:,}"

            col_rows += (
                f'<tr style="background:{bg}">'
                f"<td><code>{_html.escape(col.name)}</code></td>"
                f"<td>{_html.escape(col.data_type)}</td>"
                f"<td style='text-align:right'>{col.total_count:,}</td>"
                f"<td style='text-align:right'>{_pct_str(col.null_percentage)}</td>"
                f"<td style='text-align:right'>{unique_str}</td>"
                f"<td>{_html.escape(stats)}</td>"
                f"<td>{pattern}</td>"
                f"</tr>"
            )
        return (
            "<div style='font-family:sans-serif;max-width:100%;overflow-x:auto'>"
            "<h3>ProfileReport</h3>"
            f"<p><b>Source:</b> {_html.escape(self.source)} | "
            f"<b>Rows:</b> {self.rows:,} | "
            f"<b>Columns:</b> {self.columns} | "
            f"<b>Quality:</b> {qs_html} | "
            f"<b>Time:</b> {self.execution_time_ms}ms</p>"
            f"{note_html}"
            "<table style='border-collapse:collapse;width:100%'>"
            "<tr style='border-bottom:2px solid #e5e7eb'>"
            "<th style='text-align:left;padding:4px 8px'>Column</th>"
            "<th style='text-align:left;padding:4px 8px'>Type</th>"
            "<th style='text-align:right;padding:4px 8px'>Count</th>"
            "<th style='text-align:right;padding:4px 8px'>Null %</th>"
            "<th style='text-align:right;padding:4px 8px'>Unique</th>"
            "<th style='text-align:left;padding:4px 8px'>Stats</th>"
            "<th style='text-align:left;padding:4px 8px'>Pattern</th>"
            "</tr>"
            f"{col_rows}</table></div>"
        )


# Preserve the public import path for introspection and pickled API objects.
ProfileReport.__module__ = "dataprof"
