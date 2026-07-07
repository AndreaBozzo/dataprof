"""dataprof - High-performance data profiling library."""

from __future__ import annotations

import csv as _csv
import decimal as _decimal
import errno as _errno
import functools
import html as _html
import io as _io
import json
import math
import os
import pathlib
import warnings
from collections.abc import Callable, Iterator
from typing import Any, cast

from ._dataprof import (
    ColumnProfile,
    DataQualityMetrics,
    ProfilerConfig,
    ProfileReport as _RustProfileReport,
    ProgressEvent,
    RecordBatch,
    RowCountEstimate,
    SamplingStrategy,
    SchemaResult,
    StopCondition,
    StructureColumnSummary,
    StructureReport,
    __version__,
    analyze_file as _analyze_file,
    analyze_structure as _analyze_structure,
    infer_schema as _infer_schema,
    profile_arrow as _profile_arrow,
    profile_dataframe as _profile_dataframe,
    quick_row_count as _quick_row_count,
)

__all__ = [
    "profile",
    "Profiler",
    "ProfileReport",
    "ProfilerConfig",
    "ColumnProfile",
    "column_to_dict",
    "DataQualityMetrics",
    "SamplingStrategy",
    "StopCondition",
    "ProgressEvent",
    "infer_schema",
    "quick_row_count",
    "analyze_structure",
    "SchemaResult",
    "RowCountEstimate",
    "StructureColumnSummary",
    "StructureReport",
    "RecordBatch",
    "__version__",
]


# ---------------------------------------------------------------------------
# Rounding helpers — match the convention in dataprof-core serde helpers.
# ---------------------------------------------------------------------------


def _half_up(v: float, ndigits: int) -> float:
    """Round using half-away-from-zero, matching Rust f64::round() semantics.

    Python's built-in round() uses bankers rounding (ties-to-even), which can
    disagree with the Rust report serialization on edge cases like 1.005.
    """
    with _decimal.localcontext() as ctx:
        ctx.rounding = _decimal.ROUND_HALF_UP
        try:
            d = _decimal.Decimal(str(v)).quantize(_decimal.Decimal(10) ** -ndigits)
            return float(d)
        except _decimal.InvalidOperation:
            # Very large or very small numbers (e.g. variance ~1e+29) can't be
            # quantized to N decimal places — return as-is since rounding wouldn't
            # change the value at that magnitude anyway.
            return v


def _r2(v: float | None) -> float | None:
    """Round to 2 decimal places (percentages, ratios). None/NaN → None."""
    if v is None or not math.isfinite(v):
        return None
    return _half_up(v, 2)


def _r4(v: float | None) -> float | None:
    """Round to 4 decimal places (statistical metrics). None/NaN → None."""
    if v is None or not math.isfinite(v):
        return None
    return _half_up(v, 4)


def _round_quartiles(q: dict[str, float] | None) -> dict[str, float] | None:
    """Round quartile values to 2 decimal places."""
    if q is None:
        return None
    return {k: _half_up(v, 2) for k, v in q.items()}


def _normalize_pathlike(path: str | os.PathLike[str], *, arg_name: str = "path") -> str:
    """Normalize Python path-like input to the string form expected by Rust."""
    if isinstance(path, str):
        return path
    if isinstance(path, os.PathLike):
        normalized = os.fspath(path)
        if isinstance(normalized, str):
            return normalized
    raise TypeError(
        f"argument '{arg_name}': expected str or path-like object, "
        f"got {type(path).__module__}.{type(path).__name__}"
    )


def _require_pandas(feature: str):
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError(
            f"pandas is required to profile {feature}. "
            "Install it with: pip install dataprof[pandas]"
        ) from exc
    return pd


def _is_list_of_dicts(source: object) -> bool:
    return isinstance(source, list) and all(isinstance(row, dict) for row in source)


def _bytes_buffer(source: bytes | bytearray | memoryview | _io.BytesIO) -> _io.BytesIO:
    if isinstance(source, _io.BytesIO):
        return _io.BytesIO(source.getvalue())
    return _io.BytesIO(bytes(source))


def _normalize_existing_file(path: str | os.PathLike[str], *, arg_name: str = "path") -> str:
    normalized = _normalize_pathlike(path, arg_name=arg_name)
    try:
        pathlib.Path(normalized).stat()
    except FileNotFoundError:
        raise FileNotFoundError(_errno.ENOENT, os.strerror(_errno.ENOENT), normalized) from None
    return normalized


def infer_schema(path: str | os.PathLike[str]) -> SchemaResult:
    """Infer a file schema from a string path or path-like object."""
    return _infer_schema(_normalize_existing_file(path))


def quick_row_count(path: str | os.PathLike[str]) -> RowCountEstimate:
    """Estimate or count rows from a string path or path-like object."""
    return _quick_row_count(_normalize_existing_file(path))


def analyze_structure(
    path: str | os.PathLike[str],
    max_rows: int | None = None,
) -> StructureReport:
    """Analyze file structure with a bounded, lightweight pass."""
    return _analyze_structure(_normalize_existing_file(path), max_rows)


# ---------------------------------------------------------------------------
# Shared column record builder
# ---------------------------------------------------------------------------


def column_to_dict(col: ColumnProfile) -> dict[str, Any]:
    """Convert a single ColumnProfile to the nested dict layout used by
    :meth:`ProfileReport.to_dict`.

    The shape matches one element of ``report.to_dict()["columns"]``:

    .. code-block:: python

        {
          "name": ..., "data_type": ..., "total_count": ..., "null_count": ...,
          "null_percentage": ..., "unique_count": ..., "uniqueness_ratio": ...,
          "stats": {"min": ..., "max": ..., ...},      # numeric / text / boolean
          "patterns": [{"name": ..., "regex": ..., ...}, ...]
        }

    Floating-point values are rounded to match the Rust serialization
    (2dp for percentages, 4dp for statistics).
    """
    col_data: dict[str, Any] = {
        "name": col.name,
        "data_type": col.data_type,
        "total_count": col.total_count,
        "null_count": col.null_count,
        "null_percentage": _r2(col.null_percentage),
        "unique_count": col.unique_count,
        "uniqueness_ratio": _r2(col.uniqueness_ratio),
    }
    if col.min is not None:
        col_data["stats"] = {
            k: v
            for k, v in {
                "min": _r4(col.min),
                "max": _r4(col.max),
                "mean": _r4(col.mean),
                "std_dev": _r4(col.std_dev),
                "variance": _r4(col.variance),
                "median": _r4(col.median),
                "mode": _r4(col.mode),
                "skewness": _r4(col.skewness),
                "kurtosis": _r4(col.kurtosis),
                "coefficient_of_variation": _r4(col.coefficient_of_variation),
                "quartiles": _round_quartiles(col.quartiles),
                "is_approximate": col.is_approximate,
                "outlier_count": col.outlier_count,
            }.items()
            if v is not None
        }
    if col.min_length is not None:
        if "stats" not in col_data:
            col_data["stats"] = {}
        col_data["stats"].update(
            {
                k: v
                for k, v in {
                    "min_length": col.min_length,
                    "max_length": col.max_length,
                    "avg_length": _r4(col.avg_length),
                }.items()
                if v is not None
            }
        )
    if col.true_count is not None:
        bool_stats: dict[str, Any] = col_data.setdefault("stats", {})
        bool_stats["true_count"] = col.true_count
        bool_stats["false_count"] = col.false_count
        bool_stats["true_ratio"] = _r4(col.true_ratio)

    if col.patterns is not None:
        col_data["patterns"] = [
            {
                "name": p.name,
                "regex": p.regex,
                "match_count": p.match_count,
                "match_percentage": _r2(p.match_percentage),
                "category": p.category,
                "confidence": round(p.confidence, 4),
            }
            for p in col.patterns
        ]
    return col_data


def _column_record(col: ColumnProfile) -> dict[str, Any]:
    """Build a flat dict of all column stats with proper rounding.

    Used by to_dataframe(), to_polars(), to_arrow(), describe(), save().
    """
    q = col.quartiles
    rq = _round_quartiles(q)
    top_pattern = None
    top_pattern_pct = None
    top_pattern_category = None
    top_pattern_confidence = None
    if col.patterns:
        best = max(col.patterns, key=lambda p: p.confidence)
        top_pattern = best.name
        top_pattern_pct = _r2(best.match_percentage)
        top_pattern_category = best.category
        top_pattern_confidence = round(best.confidence, 4)

    return {
        "name": col.name,
        "data_type": col.data_type,
        "total_count": col.total_count,
        "null_count": col.null_count,
        "null_percentage": _r2(col.null_percentage),
        "unique_count": col.unique_count,
        "uniqueness_ratio": _r2(col.uniqueness_ratio),
        "min": _r4(col.min),
        "max": _r4(col.max),
        "mean": _r4(col.mean),
        "std_dev": _r4(col.std_dev),
        "variance": _r4(col.variance),
        "median": _r4(col.median),
        "mode": _r4(col.mode),
        "skewness": _r4(col.skewness),
        "kurtosis": _r4(col.kurtosis),
        "coefficient_of_variation": _r4(col.coefficient_of_variation),
        "q1": rq["q1"] if rq else None,
        "q2": rq["q2"] if rq else None,
        "q3": rq["q3"] if rq else None,
        "iqr": rq["iqr"] if rq else None,
        "is_approximate": col.is_approximate,
        "outlier_count": col.outlier_count,
        "min_length": col.min_length,
        "max_length": col.max_length,
        "avg_length": _r4(col.avg_length),
        "true_count": col.true_count,
        "false_count": col.false_count,
        "true_ratio": _r4(col.true_ratio),
        "top_pattern": top_pattern,
        "top_pattern_pct": top_pattern_pct,
        "top_pattern_category": top_pattern_category,
        "top_pattern_confidence": top_pattern_confidence,
    }


def _stats_cell(col: ColumnProfile) -> str:
    """Compact stats summary for a column, shared by the HTML/markdown/text views.

    Numeric columns show mean/std/median; boolean columns show true count and
    ratio; text columns show average length. Returns "" when none apply.
    """
    parts: list[str] = []
    if col.mean is not None:
        parts.append(f"mean={_r4(col.mean)}")
        if col.std_dev is not None:
            parts.append(f"std={_r4(col.std_dev)}")
        if col.median is not None:
            parts.append(f"median={_r4(col.median)}")
    elif col.true_count is not None:
        pct = _r2(col.true_ratio * 100) if col.true_ratio is not None else 0
        parts.append(f"true={col.true_count} ({pct:.0f}%)")
    elif col.avg_length is not None:
        parts.append(f"avg_len={_r4(col.avg_length)}")
    return ", ".join(parts)


def _pattern_cell(col: ColumnProfile) -> str:
    """Highest-confidence detected pattern for a column, as "Name (pct%)"."""
    if col.patterns:
        best = max(col.patterns, key=lambda p: p.confidence)
        return f"{best.name} ({_r2(best.match_percentage):.0f}%)"
    return ""


def profile(
    source: Any,
    *,
    engine: str = "auto",
    chunk_size: int | None = None,
    memory_limit_mb: int | None = None,
    format: str | None = None,
    max_rows: int | None = None,
    name: str | None = None,
    csv_delimiter: str | None = None,
    csv_flexible: bool | None = None,
    sampling: SamplingStrategy | None = None,
    stop_condition: StopCondition | None = None,
    on_progress: Callable[[ProgressEvent], None] | None = None,
    progress_interval_ms: int | None = None,
    quality_dimensions: list[str] | None = None,
    metrics: list[str] | None = None,
    locale: str | None = None,
    positive_columns: list[str] | None = None,
    identifier_columns: list[str] | None = None,
) -> ProfileReport:
    """Profile a data source and return a report.

    Accepts file paths (str/Path), pandas DataFrames, polars DataFrames,
    Arrow PyCapsule-compatible objects, dict/list-of-dicts, and bytes-like
    file contents when ``format=`` is provided.

    Args:
        source: Data source to profile.
        engine: Engine to use ("auto", "incremental", "columnar").
        chunk_size: Fixed chunk size for streaming (None = adaptive).
        memory_limit_mb: Memory limit in MB.
        format: Override format detection ("csv", "json", "jsonl", "parquet").
        max_rows: Maximum rows to process before stopping.
        name: Name for DataFrame sources in the report.
        csv_delimiter: Single-character CSV delimiter (default: comma).
        csv_flexible: Allow variable-length CSV records.
        sampling: Sampling strategy (e.g. SamplingStrategy.random(1000)).
        stop_condition: Early stop condition (e.g. StopCondition.max_rows(5000)).
            Cannot be used together with max_rows.
        on_progress: Callable receiving ProgressEvent objects during profiling.
            Only effective with engine="incremental".
        progress_interval_ms: Minimum interval between progress events in ms
            (default: 500).
        quality_dimensions: List of ISO 25012 quality dimensions to evaluate.
            Valid values: "completeness", "consistency", "uniqueness",
            "accuracy", "timeliness". None = all dimensions (default).
        metrics: List of metric packs to compute. Valid values: "schema"
            (always included), "statistics", "patterns", "quality".
            None = all packs (default). Omitting a pack skips that
            category of computation entirely.
        locale: ISO 3166-1 alpha-2 locale for pattern detection (e.g. "IT",
            "US", "GB"). Boosts confidence for locale-matching patterns and
            suppresses non-matching locale patterns. None = no preference.
        positive_columns: Columns whose numeric values are expected to be
            non-negative.
        identifier_columns: Numeric-looking columns to treat as semantic
            identifiers instead of measures.

    Returns:
        ProfileReport with analysis results and quality metrics.
    """
    # File path — build config and delegate to Rust
    if isinstance(source, (str, pathlib.PurePath)):
        path = _normalize_existing_file(source, arg_name="source")
        config = ProfilerConfig(
            engine=engine,
            chunk_size=chunk_size,
            memory_limit_mb=memory_limit_mb,
            format=format,
            max_rows=max_rows,
            csv_delimiter=csv_delimiter,
            csv_flexible=csv_flexible,
            sampling=sampling,
            stop_condition=stop_condition,
            on_progress=on_progress,
            progress_interval_ms=progress_interval_ms,
            quality_dimensions=quality_dimensions,
            metrics=metrics,
            locale=locale,
            positive_columns=positive_columns,
            identifier_columns=identifier_columns,
        )
        rust_report = _analyze_file(path, config)
        return ProfileReport(rust_report)

    # DataFrame/Arrow paths — build config for metric packs + quality dims + locale
    def _df_config() -> ProfilerConfig | None:
        """Build a ProfilerConfig if any DataFrame-relevant options are set."""
        if any(
            v is not None
            for v in (
                max_rows,
                quality_dimensions,
                metrics,
                locale,
                positive_columns,
                identifier_columns,
            )
        ):
            return ProfilerConfig(
                max_rows=max_rows,
                quality_dimensions=quality_dimensions,
                metrics=metrics,
                locale=locale,
                positive_columns=positive_columns,
                identifier_columns=identifier_columns,
            )
        return None

    # Warn about file-only kwargs that are ignored for DataFrame/Arrow sources
    _file_only_kwargs = {
        "engine": engine != "auto",
        "chunk_size": chunk_size is not None,
        "memory_limit_mb": memory_limit_mb is not None,
        "format": format is not None,
        "csv_delimiter": csv_delimiter is not None,
        "csv_flexible": csv_flexible is not None,
        "sampling": sampling is not None,
        "stop_condition": stop_condition is not None,
        "on_progress": on_progress is not None,
        "progress_interval_ms": progress_interval_ms is not None,
    }

    def _warn_if_config_ignored():
        ignored = [k for k, v in _file_only_kwargs.items() if v]
        if ignored:
            warnings.warn(
                f"Config kwargs {ignored} are ignored for DataFrame/Arrow sources. "
                "These options only apply to file paths.",
                stacklevel=3,
            )

    def _profile_python_dataframe(df: object, default_name: str) -> ProfileReport:
        rust_report = _profile_dataframe(df, name or default_name, max_rows, _df_config())
        return ProfileReport(rust_report)

    if isinstance(source, dict) or _is_list_of_dicts(source):
        _warn_if_config_ignored()
        pd = _require_pandas("dict and list-of-dicts inputs")
        return _profile_python_dataframe(pd.DataFrame(source), "dataframe")

    if isinstance(source, (bytes, bytearray, memoryview, _io.BytesIO)):
        if format is None:
            raise ValueError(
                "bytes and BytesIO sources require format='csv', 'json', 'jsonl', or 'parquet'. "
                "For async byte streams, use dataprof.asyncio.profile_bytes(data, format='csv')."
            )
        pd = _require_pandas("bytes and BytesIO inputs")
        fmt = format.lower()
        buffer = _bytes_buffer(source)
        if fmt == "csv":
            df = pd.read_csv(buffer, sep=csv_delimiter or ",")
        elif fmt == "json":
            df = pd.read_json(buffer)
        elif fmt == "jsonl":
            df = pd.read_json(buffer, lines=True)
        elif fmt == "parquet":
            df = pd.read_parquet(buffer)
        else:
            raise ValueError("Unsupported bytes format. Use 'csv', 'json', 'jsonl', or 'parquet'.")
        return _profile_python_dataframe(df, f"{fmt}_bytes")

    # DataFrame detection via module name
    source_module = type(source).__module__ or ""

    if source_module.startswith("pandas"):
        _warn_if_config_ignored()
        return _profile_python_dataframe(source, "dataframe")

    if source_module.startswith("polars"):
        _warn_if_config_ignored()
        return _profile_python_dataframe(source, "dataframe")

    # PyArrow objects (Table, RecordBatch) or any Arrow PyCapsule-compatible object
    if source_module.startswith("pyarrow") or hasattr(source, "__arrow_c_array__"):
        _warn_if_config_ignored()
        rust_report = _profile_arrow(source, name or "arrow_data", max_rows, _df_config())
        return ProfileReport(rust_report)

    raise TypeError(
        f"Unsupported source type: {type(source).__module__}.{type(source).__name__}. "
        "Expected a file path (str/Path), pandas DataFrame, polars DataFrame, "
        "an object implementing the Arrow PyCapsule protocol, dict/list-of-dicts, "
        "or bytes/BytesIO with format=."
    )


# Stop-when shorthand strings for the Profiler builder
_STOP_SHORTHANDS: dict[str, Callable[[], StopCondition]] = {
    "schema_stable": lambda: StopCondition.schema_stable(1000),
    "schema_inference": lambda: StopCondition.schema_inference(),
    "quality_sample": lambda: StopCondition.quality_sample(),
}

# Valid metric pack names
_VALID_METRIC_PACKS = {"schema", "statistics", "patterns", "quality"}


class Profiler:
    """Builder-style profiler configuration.

    Chainable methods accumulate settings; call ``.profile(source)`` to run.

    Example::

        report = dp.Profiler().engine("incremental").max_rows(5000).profile("data.csv")
        report = dp.Profiler().stop_when("schema_stable").profile(df)
        report = dp.Profiler().metrics(["quality"]).profile("data.csv")
    """

    def __init__(self) -> None:
        self._kwargs: dict[str, Any] = {}

    def engine(self, engine: str) -> Profiler:
        """Set profiling engine ("auto", "incremental", "columnar")."""
        self._kwargs["engine"] = engine
        return self

    def chunk_size(self, n: int) -> Profiler:
        """Set fixed chunk size for streaming (None = adaptive)."""
        self._kwargs["chunk_size"] = n
        return self

    def memory_limit_mb(self, mb: int) -> Profiler:
        """Set memory limit in MB."""
        self._kwargs["memory_limit_mb"] = mb
        return self

    def format(self, fmt: str) -> Profiler:
        """Override format detection ("csv", "json", "jsonl", "parquet")."""
        self._kwargs["format"] = fmt
        return self

    def max_rows(self, n: int) -> Profiler:
        """Set maximum rows to process."""
        self._kwargs["max_rows"] = n
        return self

    def name(self, name: str) -> Profiler:
        """Set name for DataFrame sources in the report."""
        self._kwargs["name"] = name
        return self

    def csv_delimiter(self, d: str) -> Profiler:
        """Set single-character CSV delimiter."""
        self._kwargs["csv_delimiter"] = d
        return self

    def csv_flexible(self, flexible: bool) -> Profiler:
        """Allow variable-length CSV records."""
        self._kwargs["csv_flexible"] = flexible
        return self

    def sampling(self, strategy: SamplingStrategy) -> Profiler:
        """Set sampling strategy."""
        self._kwargs["sampling"] = strategy
        return self

    def stop_condition(self, cond: StopCondition) -> Profiler:
        """Set early stop condition."""
        self._kwargs["stop_condition"] = cond
        return self

    def on_progress(self, cb: object) -> Profiler:
        """Set progress callback (engine="incremental" only)."""
        self._kwargs["on_progress"] = cb
        return self

    def progress_interval_ms(self, ms: int) -> Profiler:
        """Set minimum interval between progress events in ms."""
        self._kwargs["progress_interval_ms"] = ms
        return self

    def quality_dimensions(self, dims: list[str]) -> Profiler:
        """Select ISO 25012 quality dimensions to evaluate."""
        self._kwargs["quality_dimensions"] = dims
        return self

    def stop_when(self, condition: StopCondition | str) -> Profiler:
        """Set stop condition from a StopCondition or a shorthand string.

        Shorthand strings: "schema_stable", "schema_inference", "quality_sample".
        """
        if isinstance(condition, str):
            factory = _STOP_SHORTHANDS.get(condition)
            if factory is None:
                raise ValueError(
                    f"Unknown stop_when shorthand: {condition!r}. "
                    f"Valid shorthands: {sorted(_STOP_SHORTHANDS)}"
                )
            condition = factory()
        self._kwargs["stop_condition"] = condition
        return self

    def locale(self, locale: str) -> Profiler:
        """Set locale for pattern detection (e.g. "IT", "US", "GB")."""
        self._kwargs["locale"] = locale
        return self

    def positive_columns(self, columns: list[str]) -> Profiler:
        """Mark columns whose numeric values are expected to be non-negative."""
        self._kwargs["positive_columns"] = columns
        return self

    def identifier_columns(self, columns: list[str]) -> Profiler:
        """Mark columns to profile as semantic identifiers."""
        self._kwargs["identifier_columns"] = columns
        return self

    def metrics(self, packs: list[str]) -> Profiler:
        """Select metric packs to compute.

        Valid packs: "schema" (always included), "statistics", "patterns", "quality".
        Omitting a pack skips that category of computation entirely.
        """
        normalized_packs = [pack.lower() for pack in packs]
        unknown = set(normalized_packs) - _VALID_METRIC_PACKS
        if unknown:
            raise ValueError(
                f"Unknown metric packs: {sorted(unknown)}. "
                f"Valid packs: {sorted(_VALID_METRIC_PACKS)}"
            )
        self._kwargs["metrics"] = normalized_packs
        return self

    def profile(self, source: Any) -> ProfileReport:
        """Profile the given source with accumulated settings.

        Accepts file paths (str/Path), pandas DataFrames, polars DataFrames,
        or any object implementing the Arrow PyCapsule protocol.
        """
        return profile(source, **self._kwargs)

    def __repr__(self) -> str:
        settings = ", ".join(f"{k}={v!r}" for k, v in self._kwargs.items())
        return f"Profiler({settings})"


# ---------------------------------------------------------------------------
# Read-only proxy backing ProfileReport.from_dict() / from_json().
#
# The native ProfileReport can't be constructed from Python, so these classes
# mimic the attribute surface the export methods read (self._report.<attr> and
# self._report.column_profiles) closely enough that every ProfileReport method
# works unchanged on a reloaded report.
# ---------------------------------------------------------------------------


class _DictPattern:
    """Read-only stand-in for a native Pattern, built from a to_dict() entry."""

    def __init__(self, d: dict[str, Any]):
        self.name = d.get("name")
        self.regex = d.get("regex")
        self.match_count = d.get("match_count")
        self.match_percentage = d.get("match_percentage")
        self.category = d.get("category")
        self.confidence = d.get("confidence", 0.0)


class _DictColumn:
    """Read-only stand-in for a native ColumnProfile, built from to_dict()."""

    # Optional stat attributes, all defaulting to None; a subset is overlaid
    # from the nested "stats" dict depending on the column's data type.
    _STAT_ATTRS = (
        "min",
        "max",
        "mean",
        "std_dev",
        "variance",
        "median",
        "mode",
        "skewness",
        "kurtosis",
        "coefficient_of_variation",
        "quartiles",
        "is_approximate",
        "outlier_count",
        "min_length",
        "max_length",
        "avg_length",
        "true_count",
        "false_count",
        "true_ratio",
    )

    def __init__(self, d: dict[str, Any]):
        for attr in self._STAT_ATTRS:
            setattr(self, attr, None)
        self.name = d.get("name")
        self.data_type = d.get("data_type")
        self.total_count = d.get("total_count")
        self.null_count = d.get("null_count")
        self.null_percentage = d.get("null_percentage")
        self.unique_count = d.get("unique_count")
        self.uniqueness_ratio = d.get("uniqueness_ratio")
        # Overlay only known stat attributes — never setattr arbitrary keys from
        # (possibly malformed) input, which could inject unexpected/dunder names.
        stats = d.get("stats")
        if isinstance(stats, dict):
            allowed = set(self._STAT_ATTRS)
            for key, value in stats.items():
                if key in allowed:
                    setattr(self, key, value)
        patterns = d.get("patterns")
        self.patterns = (
            [_DictPattern(p) for p in patterns if isinstance(p, dict)]
            if isinstance(patterns, list)
            else None
        )


class _DictQuality:
    """Read-only stand-in for native DataQualityMetrics, built from to_dict()."""

    def __init__(self, d: dict[str, Any]):
        self._d = d
        self.low_sample_warning = bool(d.get("low_sample_warning", False))

    @property
    def completeness(self) -> dict[str, Any] | None:
        return self._d.get("completeness")

    @property
    def consistency(self) -> dict[str, Any] | None:
        return self._d.get("consistency")

    @property
    def uniqueness(self) -> dict[str, Any] | None:
        return self._d.get("uniqueness")

    @property
    def accuracy(self) -> dict[str, Any] | None:
        return self._d.get("accuracy")

    @property
    def timeliness(self) -> dict[str, Any] | None:
        return self._d.get("timeliness")

    def overall_quality_score(self) -> float | None:
        return self._d.get("overall_score")


class _DictBackedReport:
    """Read-only stand-in for the native ProfileReport, built from to_dict()."""

    def __init__(self, d: dict[str, Any]):
        execution = d.get("execution") or {}
        self.source = d.get("source")
        self.source_type = d.get("source_type")
        self.rows_processed = execution.get("rows_processed")
        self.columns_detected = execution.get("columns_detected")
        self.scan_time_ms = execution.get("scan_time_ms")
        self.source_exhausted = execution.get("source_exhausted")
        self.truncation_reason = execution.get("truncation_reason")
        self.bytes_consumed = execution.get("bytes_consumed")
        self.throughput_rows_sec = execution.get("throughput_rows_sec")
        self.memory_peak_mb = execution.get("memory_peak_mb")
        self.error_count = execution.get("error_count")
        self.sampling_applied = bool(execution.get("sampling_applied", False))
        self.sampling_ratio = execution.get("sampling_ratio")
        quality = d.get("quality")
        self.quality = _DictQuality(quality) if isinstance(quality, dict) else None
        self.quality_score = quality.get("overall_score") if isinstance(quality, dict) else None
        self.column_profiles = [_DictColumn(c) for c in d.get("columns", [])]


class ProfileReport:
    """High-level wrapper around the Rust ProfileReport with export methods.

    Supports dict-like column access::

        report["column_name"]          # -> ColumnProfile
        "column_name" in report        # -> bool
        for name in report: ...        # iterate column names
        len(report)                    # number of columns
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
    def rows(self) -> int:
        return self._report.rows_processed

    @property
    def columns(self) -> int:
        return self._report.columns_detected

    @functools.cached_property
    def column_profiles(self) -> dict[str, ColumnProfile]:
        cols = self._report.column_profiles
        d = {col.name: col for col in cols}
        if len(d) != len(cols):
            warnings.warn(
                f"Dataset has duplicate column names — {len(cols) - len(d)} "
                "column(s) shadowed in dict access. Use _report.column_profiles "
                "for the full list.",
                stacklevel=2,
            )
        return d

    @property
    def quality_score(self) -> float | None:
        v = self._report.quality_score
        return _r2(v)

    @property
    def quality(self) -> DataQualityMetrics | None:
        return self._report.quality

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
    def sampling_applied(self) -> bool:
        return self._report.sampling_applied

    @property
    def sampling_ratio(self) -> float | None:
        return _r4(self._report.sampling_ratio)

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

    def __iter__(self) -> Iterator[str]:
        return iter(self.column_profiles)

    def __len__(self) -> int:
        return self.columns

    # -- Export methods --

    def to_dict(self) -> dict:
        """Convert the report to a nested Python dict.

        All floating-point values are rounded (2dp for percentages, 4dp for
        statistics) to match the Rust report serialization.
        """
        cols = [column_to_dict(col) for col in self._report.column_profiles]

        quality_dict = None
        q = self._report.quality
        if q is not None:
            quality_dict = {
                "overall_score": _r2(q.overall_quality_score()),
            }
            if q.low_sample_warning:
                quality_dict["low_sample_warning"] = True
            comp = q.completeness
            if comp is not None:
                quality_dict["completeness"] = comp
            cons = q.consistency
            if cons is not None:
                quality_dict["consistency"] = cons
            uniq = q.uniqueness
            if uniq is not None:
                quality_dict["uniqueness"] = uniq
            acc = q.accuracy
            if acc is not None:
                quality_dict["accuracy"] = acc
            tim = q.timeliness
            if tim is not None:
                quality_dict["timeliness"] = tim

        return {
            "source": self._report.source,
            "source_type": self._report.source_type,
            "execution": {
                "rows_processed": self._report.rows_processed,
                "columns_detected": self._report.columns_detected,
                "scan_time_ms": self._report.scan_time_ms,
                "source_exhausted": self._report.source_exhausted,
                "truncation_reason": self._report.truncation_reason,
                "bytes_consumed": self._report.bytes_consumed,
                "throughput_rows_sec": _r4(self._report.throughput_rows_sec),
                "memory_peak_mb": _r2(self._report.memory_peak_mb),
                "error_count": self._report.error_count,
                "sampling_applied": self._report.sampling_applied,
                "sampling_ratio": _r4(self._report.sampling_ratio),
            },
            "columns": cols,
            "quality": quality_dict,
        }

    def to_json(self, indent: int = 2) -> str:
        """Export the report as a JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    def _records(self) -> list[dict[str, Any]]:
        """Build enriched, rounded records for all columns."""
        return [_column_record(col) for col in self._report.column_profiles]

    def to_dataframe(self):
        """Convert column profiles to a pandas DataFrame.

        Includes all available statistics (numeric, text, pattern) with
        proper rounding. Requires pandas.
        """
        import pandas as pd

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

    def describe(self) -> Any:
        """Transposed statistical summary, similar to pandas DataFrame.describe().

        Rows are stats (count, null%, unique, mean, std, min, 25%, 50%, 75%,
        max, min_length, max_length, avg_length). Columns are dataset columns.
        Returns a pandas DataFrame if pandas is available, otherwise a
        dict-of-dicts.
        """
        summary: dict[str, dict[str, Any]] = {}
        for col in self._report.column_profiles:
            q = _round_quartiles(col.quartiles)
            d: dict[str, Any] = {
                "count": col.total_count,
                "null%": _r2(col.null_percentage),
                "unique": col.unique_count,
                "mean": _r4(col.mean),
                "std": _r4(col.std_dev),
                "min": _r4(col.min),
                "25%": q["q1"] if q else None,
                "50%": q["q2"] if q else None,
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

    def quality_summary(self) -> dict[str, Any]:
        """Single-row quality summary for easy aggregation.

        Returns a dict with source, rows, quality_score, per-dimension scores,
        and execution_time_ms. Useful for pd.concat() across multiple reports.
        """
        q = self._report.quality
        row: dict[str, Any] = {
            "source": self._report.source,
            "rows": self._report.rows_processed,
            "quality_score": _r2(self._report.quality_score),
            "completeness": None,
            "consistency": None,
            "uniqueness": None,
            "accuracy": None,
            "timeliness": None,
            "execution_time_ms": self._report.scan_time_ms,
        }
        if q is not None:
            comp = q.completeness
            if comp is not None:
                row["completeness"] = _r2(comp.get("complete_records_ratio"))
            cons = q.consistency
            if cons is not None:
                row["consistency"] = _r2(cons.get("data_type_consistency"))
            uniq = q.uniqueness
            if uniq is not None:
                row["uniqueness"] = _r2(uniq.get("key_uniqueness"))
            acc = q.accuracy
            if acc is not None:
                # Invert: low outlier_ratio → high accuracy score
                raw = 100.0 - (acc.get("outlier_ratio") or 0.0)
                row["accuracy"] = _r2(max(0.0, min(100.0, raw)))
            tim = q.timeliness
            if tim is not None:
                # Invert: low stale_data_ratio → high timeliness score
                raw = 100.0 - (tim.get("stale_data_ratio") or 0.0)
                row["timeliness"] = _r2(max(0.0, min(100.0, raw)))
        return row

    def save(self, path: str) -> ProfileReport:
        """Save the report to a file.

        Supported formats (by extension):
            .json    — full report as JSON
            .csv     — column profiles as CSV (no extra dependencies)
            .parquet — column profiles as Parquet (requires pyarrow)

        Args:
            path: File path with one of the supported extensions.
        """
        if path.endswith(".json"):
            with open(path, "w", encoding="utf-8") as f:
                f.write(self.to_json())
        elif path.endswith(".csv"):
            records = self._records()
            if records:
                with open(path, "w", encoding="utf-8", newline="") as f:
                    writer = _csv.DictWriter(f, fieldnames=records[0].keys())
                    writer.writeheader()
                    writer.writerows(records)
        elif path.endswith(".parquet"):
            table = self.to_arrow()
            import pyarrow.parquet as pq

            pq.write_table(table, path)
        else:
            raise ValueError("Unsupported format. Use .json, .csv, or .parquet")
        return self

    def to_html(self) -> str:
        """Return the standalone HTML representation of the report.

        Identical to Jupyter's rich display (``_repr_html_``), exposed as a
        public method for saving standalone HTML files, embedding in CI
        summaries, or sharing outside a notebook.
        """
        return self._repr_html_()

    def to_markdown(self) -> str:
        """Render the report as a GitHub-flavored markdown table.

        Suitable for issue bodies, pull-request comments, Slack posts, or
        README snippets. Uses the same per-column summary as the HTML view.
        """

        def esc(value: object) -> str:
            return str(value).replace("|", "\\|")

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
            unique_str = f"{col.unique_count:,}" if col.unique_count is not None else ""
            row = [
                esc(col.name),
                esc(col.data_type),
                f"{col.total_count:,}",
                f"{_r2(col.null_percentage):.1f}%",
                unique_str,
                esc(_stats_cell(col)),
                esc(_pattern_cell(col)),
            ]
            lines.append("| " + " | ".join(row) + " |")
        return "\n".join(lines)

    def compare(self, other: ProfileReport) -> dict[str, Any]:
        """Compare this report with another and return a dict of deltas.

        The result captures quality drift and schema differences between two
        profiles (e.g. the same dataset before and after a pipeline change):

        - ``quality_score``: overall score for each side plus absolute and
          relative-percent change.
        - ``dimensions``: the same a/b/abs/rel_pct shape per ISO 25012
          dimension (completeness, consistency, uniqueness, accuracy,
          timeliness), sourced from :meth:`quality_summary`.
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
            for dim in ("completeness", "consistency", "uniqueness", "accuracy", "timeliness")
        }

        a_cols = self.column_profiles
        b_cols = other.column_profiles
        a_names = set(a_cols)
        b_names = set(b_cols)

        def _null_pct(cols: dict[str, ColumnProfile], name: str) -> float | None:
            col = cols.get(name)
            return _r2(col.null_percentage) if col is not None else None

        columns: dict[str, dict[str, float | None]] = {}
        for name in sorted(a_names | b_names):
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
                "added": sorted(b_names - a_names),
                "removed": sorted(a_names - b_names),
                "common": sorted(a_names & b_names),
            },
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ProfileReport:
        """Rebuild a read-only ProfileReport from a dict produced by :meth:`to_dict`.

        The reconstructed report is backed by a lightweight proxy rather than
        the native engine, so it is read-only, but all export methods
        (``to_json``, ``to_markdown``, ``to_dataframe``, ``describe``,
        ``quality_summary``, mapping access, …) work as usual. Useful for
        reloading a report saved yesterday without re-profiling the data.

        Raises:
            ValueError: if ``data`` is not a mapping produced by ``to_dict()``.
        """
        if not isinstance(data, dict) or not {"source", "columns", "execution"} <= data.keys():
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
        return cls(cast("_RustProfileReport", _DictBackedReport(data)))

    @classmethod
    def from_json(cls, text: str) -> ProfileReport:
        """Rebuild a read-only ProfileReport from JSON produced by :meth:`to_json`.

        See :meth:`from_dict` for the reconstruction semantics.

        Raises:
            ValueError: if ``text`` is not valid JSON from ``to_json()``.
        """
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"from_json() received invalid JSON: {exc}") from exc
        return cls.from_dict(data)

    @classmethod
    def load(cls, path: str | os.PathLike[str]) -> ProfileReport:
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
        if path.endswith(".json"):
            with open(path, encoding="utf-8") as f:
                return cls.from_json(f.read())
        if path.endswith((".csv", ".parquet")):
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
            parts.append(f"{_r2(col.null_percentage):>5.1f}% null")
            if col.unique_count is not None:
                parts.append(f"{col.unique_count:,} unique")
            if col.mean is not None:
                parts.append(f"mean={_r4(col.mean)}")
                if col.std_dev is not None:
                    parts.append(f"std={_r4(col.std_dev)}")
            elif col.true_count is not None:
                pct = _r2(col.true_ratio * 100) if col.true_ratio is not None else 0
                parts.append(f"true={col.true_count}({pct:.0f}%)")
            elif col.avg_length is not None:
                parts.append(f"avg_len={_r4(col.avg_length)}")
            if col.patterns:
                best = max(col.patterns, key=lambda p: p.confidence)
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
            unique_str = f"{col.unique_count:,}" if col.unique_count is not None else ""

            col_rows += (
                f'<tr style="background:{bg}">'
                f"<td><code>{_html.escape(col.name)}</code></td>"
                f"<td>{_html.escape(col.data_type)}</td>"
                f"<td style='text-align:right'>{col.total_count:,}</td>"
                f"<td style='text-align:right'>{_r2(col.null_percentage):.1f}%</td>"
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
