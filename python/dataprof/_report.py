"""Public report access, rendering, comparison, and persistence."""

from __future__ import annotations as _annotations

import csv as _csv
import functools as _functools
import html as _html
import json as _json
import os as _os
import warnings as _warnings
from collections.abc import Iterator as _Iterator
from typing import Any as _Any, cast as _cast

from ._columns import _column_record, _dominant_pattern, column_to_dict
from ._dataprof import ColumnProfile, DataQualityMetrics, ProfileReport as _RustProfileReport
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
from ._report_backing import _DictBackedReport
from ._report_schema import _QUALITY_DIMENSIONS, REPORT_SCHEMA_VERSION
from ._rounding import _r2, _r4, _round_dimension, _round_quartiles


def _quality_status_document(state: str, error: str | None) -> dict[str, _Any]:
    """Serialize the quality-computation outcome.

    One object, one fact, the same shape the Rust dialect writes. ``error`` is
    present only on ``failed``, where it is the whole point of the record.
    """
    document: dict[str, _Any] = {"state": state}
    if error is not None:
        document["error"] = error
    return document


class ProfileReport:
    """High-level wrapper around the Rust ProfileReport with export methods.

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

    def __init__(self, report: _RustProfileReport):
        self._report = report

    # -- Property accessors --

    @property
    def source(self) -> str:
        return self._report.source

    @property
    def source_type(self) -> str:
        return self._report.source_type

    @property
    def engine(self) -> str | None:
        return self._report.engine

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
    def semantic_hint_bindings(self) -> list[dict[str, _Any]]:
        """Per-column evidence of how each semantic hint bound to the data.

        Empty unless ``positive_columns``/``identifier_columns``/
        ``temporal_columns`` were supplied. Each entry has ``column``, ``kind``,
        ``checked_values``, ``matched_values``, and ``exact``. A hint proven
        inert over the full data raises before a report is returned, so a report
        only ever carries bindings that matched something or whose evidence was
        sampled.
        """
        return list(getattr(self._report, "semantic_hint_bindings", []))

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
        4dp for statistics and for ``0..1`` ratios such as ``uniqueness_ratio``.
        The document carries ``schema_version`` (``dataprof.REPORT_SCHEMA_VERSION``) so
        saved reports remain readable across releases; see
        :meth:`from_dict` for the compatibility policy.
        """
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
            }
            # Always emit: a non-optional bool (False = "sample was adequate")
            # so consumers never have to infer absence, and from_dict round-trips
            # both states. See docs/python/README.md report-schema notes.
            quality_dict["low_sample_warning"] = bool(q.low_sample_warning)
            # The dimension dicts used to be passed through raw while the Rust
            # serializer rounded every float in them to 2dp, so the two layers
            # reported different numbers for the same field — 4.833333333333333
            # here against 4.83 there (#513).
            for dimension in _QUALITY_DIMENSIONS:
                values = getattr(q, dimension)
                if values is not None:
                    quality_dict[dimension] = _round_dimension(values)

        document: dict[str, _Any] = {
            "schema_version": REPORT_SCHEMA_VERSION,
            "source": self._report.source,
            "source_type": self._report.source_type,
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
                "ragged_row_count": self._report.ragged_row_count,
                "sampling_applied": self._report.sampling_applied,
                "sampling_ratio": _r4(self._report.sampling_ratio),
            },
            "columns": cols,
            "quality": quality_dict,
            # Why `quality` is or is not there. Always emitted: a report whose
            # quality computation failed must not read back as one that never
            # asked for quality.
            "quality_status": _quality_status_document(self.quality_status, self.quality_error),
        }
        # Additive provenance is omitted when the input path did not record it.
        ranges = self.sampled_row_ranges
        if ranges is not None:
            _cast(dict[str, _Any], document["execution"])["sampled_row_ranges"] = ranges
        # Additive: only present when hints were supplied, so hint-free reports
        # keep their existing shape.
        bindings = self.semantic_hint_bindings
        if bindings:
            document["semantic_hint_bindings"] = bindings
        return document

    def to_json(self, indent: int = 2) -> str:
        """Export the report as a JSON string."""
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
                "mean": _r4(col.mean),
                "std": _r4(col.std_dev),
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
            with open(path, "w", encoding="utf-8") as f:
                f.write(self.to_json())
        elif lower_path.endswith(".csv"):
            records = self._records()
            # Opening unconditionally matters for a zero-column report: save()
            # must either create the requested artifact or fail, never return
            # success while leaving no file behind.
            with open(path, "w", encoding="utf-8", newline="") as f:
                if records:
                    writer = _csv.DictWriter(f, fieldnames=records[0].keys())
                    writer.writeheader()
                    writer.writerows(records)
        elif lower_path.endswith(".parquet"):
            table = self.to_arrow()
            import pyarrow.parquet as pq

            pq.write_table(table, path)
        elif lower_path.endswith(".html"):
            with open(path, "w", encoding="utf-8") as f:
                f.write(self.to_html())
        elif lower_path.endswith((".md", ".markdown")):
            with open(path, "w", encoding="utf-8") as f:
                f.write(self.to_markdown())
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
        """Compare this report with another and return a dict of deltas.

        The result captures quality drift and schema differences between two
        profiles (e.g. the same dataset before and after a pipeline change):

        - ``quality_score``: overall score for each side plus absolute and
          relative-percent change.
        - ``dimensions``: the same a/b/abs/rel_pct shape per ISO 25012
          dimension (completeness, consistency, uniqueness, accuracy,
          timeliness, validity, precision), sourced from :meth:`quality_summary`.
        - ``columns``: per-column null-percentage drift over the union of
          column names (missing on one side → ``None``).
        - ``schema``: column names ``added`` / ``removed`` / ``common``.

        .. note::
            The exact shape is provisional and will align with the Rust-side
            ``QualityDelta`` type (#310) once it lands.
        """

        def _delta(a: float | None, b: float | None) -> dict[str, float | None]:
            abs_change = None if a is None or b is None else _r2(b - a)
            if a is None or b is None or a == 0:
                rel_pct = None
            else:
                rel_pct = _r2((b - a) / abs(a) * 100.0)
            return {"a": a, "b": b, "abs": abs_change, "rel_pct": rel_pct}

        a_summary = self.quality_summary()
        b_summary = other.quality_summary()

        dimensions = {
            dim: _delta(a_summary.get(dim), b_summary.get(dim))
            for dim in (
                "completeness",
                "consistency",
                "uniqueness",
                "accuracy",
                "timeliness",
                "validity",
                "precision",
            )
        }

        a_cols = self.column_profiles
        b_cols = other.column_profiles
        a_order = list(a_cols)
        b_order = list(b_cols)
        a_names = set(a_order)
        b_names = set(b_order)
        # Preserve the left report's schema order, then append right-only
        # columns in their source order.
        all_names = [*a_order, *(name for name in b_order if name not in a_names)]

        def _null_pct(cols: dict[str, ColumnProfile], name: str) -> float | None:
            col = cols.get(name)
            return _r2(col.null_percentage) if col is not None else None

        columns: dict[str, dict[str, float | None]] = {}
        for name in all_names:
            null_a = _null_pct(a_cols, name)
            null_b = _null_pct(b_cols, name)
            null_delta = None if null_a is None or null_b is None else _r2(null_b - null_a)
            columns[name] = {
                "null_pct_a": null_a,
                "null_pct_b": null_b,
                "null_pct_delta": null_delta,
            }

        return {
            "quality_score": _delta(self.quality_score, other.quality_score),
            "dimensions": dimensions,
            "columns": columns,
            "schema": {
                "added": [name for name in b_order if name not in a_names],
                "removed": [name for name in a_order if name not in b_names],
                "common": [name for name in a_order if name in b_names],
            },
        }

    @classmethod
    def from_dict(cls, data: dict[str, _Any]) -> ProfileReport:
        """Rebuild a read-only ProfileReport from a dict produced by :meth:`to_dict`.

        The reconstructed report is backed by a lightweight proxy rather than
        the native engine, so it is read-only, but all export methods
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
            if version is None or isinstance(version, bool) or not isinstance(version, int):
                raise ValueError(
                    f"from_dict(): 'schema_version' must be an integer, got {version!r}."
                )
            if version > REPORT_SCHEMA_VERSION:
                raise ValueError(
                    f"This report uses schema version {version}, but this dataprof "
                    f"reads up to version {REPORT_SCHEMA_VERSION}. Upgrade dataprof "
                    "to load it."
                )
        if not {"source", "columns", "execution"} <= data.keys():
            raise ValueError(
                "from_dict() expects a mapping produced by ProfileReport.to_dict() "
                "(with 'source', 'columns', and 'execution' keys)."
            )
        if not isinstance(data["execution"], dict):
            raise ValueError("from_dict(): 'execution' must be a mapping.")
        columns = data["columns"]
        if not isinstance(columns, list) or not all(isinstance(c, dict) for c in columns):
            raise ValueError("from_dict(): 'columns' must be a list of mappings.")
        # _DictBackedReport is a read-only proxy that duck-types the raw Rust
        # report; it intentionally isn't a nominal _RustProfileReport.
        return cls(_cast("_RustProfileReport", _DictBackedReport(data)))

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
                parts.append(f"mean={_r4(col.mean)}")
                if col.std_dev is not None:
                    parts.append(f"std={_r4(col.std_dev)}")
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
