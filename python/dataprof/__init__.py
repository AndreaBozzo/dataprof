"""dataprof - High-performance data profiling library."""

from __future__ import annotations

import csv as _csv
import decimal as _decimal
import errno as _errno
import functools
import html as _html
import importlib as _importlib
import io as _io
import json
import math
import os
import pathlib
import warnings
from collections.abc import Callable, Iterator
from dataclasses import dataclass as _dataclass
from importlib import util as _importlib_util
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
    _compiled_capabilities,
    analyze_file as _analyze_file,
    analyze_structure as _analyze_structure,
    infer_schema as _infer_schema,
    list_patterns as _list_patterns,
    profile_arrow as _profile_arrow,
    profile_columns as _profile_columns,
    profile_dataframe as _profile_dataframe,
    quick_row_count as _quick_row_count,
)

# Database helpers are compiled in only when the extension is built with the
# `database` feature and a connector. The published wheels omit them, so bind
# stubs that explain how to get them rather than leaving an AttributeError.
try:
    from ._dataprof import (  # type: ignore[import-not-found]
        analyze_database_async as _analyze_database_async,
        count_table_rows_async,
        get_table_schema_async,
        test_connection_async,
    )

    _HAS_DATABASE = True

    async def analyze_database_async(
        connection_string: str,
        query: str,
        batch_size: int = 10000,
        calculate_quality: bool = False,
    ) -> ProfileReport:
        """Profile the rows returned by ``query``.

        The native call yields a raw core report; wrap it so the result carries
        the same surface as every other ``ProfileReport`` in this package.
        """
        rust_report = await _analyze_database_async(
            connection_string, query, batch_size, calculate_quality
        )
        return ProfileReport(rust_report)

except ImportError:
    _HAS_DATABASE = False

    def _database_unavailable(name: str):
        def _stub(*_args, **_kwargs):
            raise ImportError(
                f"{name}() requires database support, which is not compiled into "
                "the published wheels. Rebuild from source with e.g. "
                "maturin develop --features 'python,python-async,database,sqlite'"
            )

        _stub.__name__ = name
        return _stub

    analyze_database_async = _database_unavailable("analyze_database_async")
    count_table_rows_async = _database_unavailable("count_table_rows_async")
    get_table_schema_async = _database_unavailable("get_table_schema_async")
    test_connection_async = _database_unavailable("test_connection_async")


@_dataclass(frozen=True, slots=True)
class Capabilities:
    """Immutable snapshot of features available in this installation."""

    version: str
    local_csv: bool
    local_json: bool
    local_jsonl: bool
    local_parquet: bool
    pandas_interop: bool
    pandas_installed: bool
    polars_interop: bool
    polars_installed: bool
    arrow_interop: bool
    pyarrow_installed: bool
    async_streaming: bool
    url_profiling: bool
    remote_parquet: bool
    database: bool
    database_connectors: tuple[str, ...]


def _dependency_installed(module_name: str) -> bool:
    """Check availability without importing the optional dependency."""
    try:
        return _importlib_util.find_spec(module_name) is not None
    except (AttributeError, ImportError, ValueError):
        return False


def capabilities() -> Capabilities:
    """Return a side-effect-free snapshot of installed dataprof capabilities.

    Optional Python packages are discovered without importing them. Compiled
    async and database support reflects the feature flags of the native module.
    """
    database = bool(_compiled_capabilities.get("database", False))
    connectors = (
        tuple(
            name
            for name in ("postgres", "mysql", "sqlite")
            if _compiled_capabilities.get(name, False)
        )
        if database
        else ()
    )
    async_streaming = bool(_compiled_capabilities.get("async_streaming", False))

    return Capabilities(
        version=__version__,
        local_csv=True,
        local_json=True,
        local_jsonl=True,
        local_parquet=True,
        pandas_interop=True,
        pandas_installed=_dependency_installed("pandas"),
        polars_interop=True,
        polars_installed=_dependency_installed("polars"),
        arrow_interop=True,
        pyarrow_installed=_dependency_installed("pyarrow"),
        async_streaming=async_streaming,
        url_profiling=async_streaming,
        remote_parquet=bool(_compiled_capabilities.get("parquet_async", False)),
        database=database,
        database_connectors=connectors,
    )


# Version of the serialized report document written by to_dict()/to_json()/
# save(). Independent of the package version: it only changes when the report
# schema itself changes. Readers accept any document whose schema_version is
# at most this value; documents without the field are legacy pre-0.10 reports
# and load through a compatibility path. See ProfileReport.from_dict().
REPORT_SCHEMA_VERSION = 1


__all__ = [
    "Capabilities",
    "capabilities",
    "REPORT_SCHEMA_VERSION",
    "analyze_database_async",
    "count_table_rows_async",
    "get_table_schema_async",
    "test_connection_async",
    "profile",
    "profile_file",
    "Profiler",
    "ProfileReport",
    "ProfilerConfig",
    "ColumnProfile",
    "DataQualityMetrics",
    "SamplingStrategy",
    "StopCondition",
    "ProgressEvent",
    "list_patterns",
    "infer_schema",
    "quick_row_count",
    "analyze_structure",
    "SchemaResult",
    "RowCountEstimate",
    "StructureColumnSummary",
    "StructureReport",
    "RecordBatch",
    "asyncio",
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


# --- Dependency-free columnar inputs (see _dataprof.profile_columns) ---
#
# dict, list-of-dicts, and decoded byte buffers all reduce to named columns of
# optional strings, which the Rust core types and profiles directly. Keeping
# these off pandas is what lets the base wheel honour its documented contract.

#: One column handed to the core: its name and its cells, `None` for null.
_Column = tuple[str, list[str | None]]


def _cell_to_str(value: object) -> str | None:
    """Render one Python cell as the string the core will type-infer.

    ``None`` and float NaN become nulls here; the core additionally treats
    null-like tokens (``""``, ``"null"``, ``"nan"``) as missing, so the result
    matches what the CSV and Arrow paths report for the same data.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        except (TypeError, ValueError):
            # Contents aren't JSON-serialisable (datetime, set, circular ref, ...);
            # fall back to str() so one odd cell doesn't abort profiling.
            return str(value)
    return str(value)


def _columns_from_dict(source: dict[Any, Any]) -> list[_Column]:
    """Convert a dict of equal-length sequences into columns."""
    columns: list[_Column] = []
    normalized_names: set[str] = set()
    for key, values in source.items():
        name = str(key)
        if name in normalized_names:
            raise ValueError(
                f"dict input: column keys collide after string conversion at {name!r}. "
                "Use distinct string column names."
            )
        normalized_names.add(name)
        if isinstance(values, (str, bytes)) or not isinstance(values, (list, tuple)):
            raise TypeError(
                f"dict input: column {key!r} must be a list or tuple of cells, "
                f"got {type(values).__name__}. For a single row, pass [{{...}}]."
            )
        columns.append((name, [_cell_to_str(v) for v in values]))

    lengths = {len(cells) for _, cells in columns}
    if len(lengths) > 1:
        widths = ", ".join(f"{n}={len(c)}" for n, c in columns)
        raise ValueError(f"dict input: columns have differing lengths ({widths}).")
    return columns


def _columns_from_records(
    rows: list[dict[Any, Any]],
    max_rows: int | None = None,
    *,
    sort_keys: bool = False,
) -> list[_Column]:
    """Convert a list of row dicts into columns, keyed in first-seen order.

    Rows need not share keys; a row missing a key contributes a null there.

    ``max_rows`` caps *key discovery* at the first ``max_rows`` rows, so a column
    that only appears past the cap is never surfaced. Cells are materialised for
    one row beyond the cap: the Rust profiler ignores that extra row (it analyses
    only ``max_rows`` of them) but uses its presence to detect that the source was
    truncated, without us stringifying every dropped row.
    """
    key_rows = rows if max_rows is None else rows[:max_rows]
    cell_rows = rows if max_rows is None else rows[: max_rows + 1]
    if sort_keys:
        keys = list(dict.fromkeys(key for row in key_rows for key in sorted(row, key=str)))
    else:
        keys = list(dict.fromkeys(key for row in key_rows for key in row))
    if key_rows and not keys:
        raise ValueError(
            "list-of-dicts input contains rows but no columns, so their row count "
            "cannot be represented. Provide at least one key or pass an empty list."
        )

    normalized_names = [str(key) for key in keys]
    if len(set(normalized_names)) != len(normalized_names):
        collisions = sorted(
            name for name in set(normalized_names) if normalized_names.count(name) > 1
        )
        raise ValueError(
            "list-of-dicts input: column keys collide after string conversion: "
            f"{collisions!r}. Use distinct string column names."
        )

    return [
        (name, [_cell_to_str(row.get(key)) for row in cell_rows])
        for key, name in zip(keys, normalized_names, strict=True)
    ]


def _columns_from_csv_bytes(buffer: _io.BytesIO, delimiter: str | None) -> list[_Column]:
    """Parse CSV bytes into columns, treating an empty field as null.

    This follows the file-based CSV engine, where an empty field is missing data
    rather than an empty string.
    """
    text = buffer.getvalue().decode("utf-8")
    if delimiter is None:
        try:
            delimiter = _csv.Sniffer().sniff(text[:8192], delimiters=",;\t|").delimiter
        except _csv.Error:
            delimiter = ","
    reader = _csv.reader(_io.StringIO(text), delimiter=delimiter)
    try:
        header = next(reader)
    except StopIteration:
        return []

    cells: list[list[str | None]] = [[] for _ in header]
    for row_num, row in enumerate(reader, start=2):
        if len(row) != len(header):
            raise ValueError(
                f"csv bytes: row {row_num} has {len(row)} fields, "
                f"expected {len(header)}. Write the data to a file to use "
                f"the flexible CSV engine (csv_flexible=True)."
            )
        for i, field in enumerate(row):
            cells[i].append(field if field != "" else None)
    return [(str(name), cells[i]) for i, name in enumerate(header)]


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


def list_patterns(locale: str | None = None) -> list[dict[str, Any]]:
    """List supported pattern detectors.

    Args:
        locale: Optional ISO 3166-1 alpha-2 locale. When provided, the result
            includes universal patterns plus locale-specific patterns matching
            the locale, case-insensitively.

    Returns:
        A list of dicts with ``name``, ``regex``, ``category``, ``locale``, and
        ``min_threshold`` keys, in detector order.
    """
    return _list_patterns(locale)


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
          "null_percentage": ..., "unique_count": ...,
          "unique_count_is_approximate": ..., "uniqueness_ratio": ...,
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
        "unique_count_is_approximate": col.unique_count_is_approximate,
        "uniqueness_ratio": _r2(col.uniqueness_ratio),
    }
    # The key is omitted entirely when the numeric/date check did not run
    # (another column type, or statistics skipped), mirroring the Rust
    # serialization; a clean checked column serializes 0.
    if col.invalid_count is not None:
        col_data["invalid_count"] = col.invalid_count
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
        "unique_count_is_approximate": col.unique_count_is_approximate,
        "uniqueness_ratio": _r2(col.uniqueness_ratio),
        "invalid_count": col.invalid_count,
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


# --- LLM context helpers (see ProfileReport.to_llm_context) ---

#: Characters per token. A deliberately rough, dependency-free approximation --
#: real tokenizers vary by model. It runs slightly conservative for prose, which
#: is the safe direction for a budget that must not be exceeded.
_CHARS_PER_TOKEN = 4

#: A column is flagged ``null-heavy`` at or above this null percentage.
_NULL_HEAVY_PCT = 20.0

#: Pattern categories whose concrete values should stay out of agent-facing
#: output even when ``include_samples=True`` asks for numeric extrema.
_SENSITIVE_PATTERN_CATEGORIES = {
    "contact",
    "identifier",
    "financial",
    "geographic",
    "network",
    "file_path",
}


def _estimate_tokens(text: str) -> int:
    """Estimate the token count of ``text`` as ``ceil(len(text) / 4)``.

    Deterministic and dependency-free. See :data:`_CHARS_PER_TOKEN`.
    """
    return -(-len(text) // _CHARS_PER_TOKEN)


_ESCAPES = {"\n": "\\n", "\r": "\\r", "\t": "\\t"}


def _one_line(value: object) -> str:
    """Render ``value`` so it cannot break the line-oriented LLM context.

    Column names and cell values come from the data, so a newline in a CSV
    header would otherwise split one schema entry across two lines -- corrupting
    the format and letting the source inject arbitrary text into an
    agent-facing summary.
    """
    text = str(value)
    return "".join(
        _ESCAPES[ch] if ch in _ESCAPES else (ch if ch.isprintable() else f"\\x{ord(ch):02x}")
        for ch in text
    )


def _may_expose_values(col: ColumnProfile) -> bool:
    """Return True only when ``col``'s raw values are *provably* safe to echo.

    Safety here is a positive claim, not the absence of a negative one. A column
    qualifies only if pattern detection actually ran (``patterns is not None``)
    and matched nothing sensitive. When detection was skipped -- ``fast_mode``,
    or a ``metrics=`` selection without the ``"patterns"`` pack -- we cannot
    tell a credit-card column from a quantity column, so we fail closed.

    Deserialized reports whose ``patterns`` key was absent also arrive as
    ``None`` and are likewise treated as unknown.
    """
    if col.patterns is None:
        return False
    return not any(
        (getattr(pattern, "category", "") or "").lower() in _SENSITIVE_PATTERN_CATEGORIES
        for pattern in col.patterns
    )


def _column_flags(col: ColumnProfile) -> list[tuple[float, str]]:
    """Derive ``(severity, text)`` quality flags for one column.

    Higher severity sorts first. Only high-signal, deterministic flags are
    emitted -- a flag that fires on almost every column is noise, not signal.
    """
    flags: list[tuple[float, str]] = []
    null_pct = col.null_percentage or 0.0
    name = _one_line(col.name)

    if null_pct >= 100.0:
        flags.append((100.0, f"{name}: all-null"))
    elif null_pct >= _NULL_HEAVY_PCT:
        flags.append((null_pct, f"{name}: null-heavy ({_r2(null_pct):.1f}% null)"))

    # A single distinct value carries no information. Suppress when the column
    # is already reported as null-heavy, where it is a restatement, not a flag.
    if col.unique_count == 1 and null_pct < _NULL_HEAVY_PCT and (col.total_count or 0) > 1:
        flags.append((60.0, f"{name}: constant (1 distinct value)"))

    outliers = col.outlier_count or 0
    total = col.total_count or 0
    if outliers > 0 and total > 0:
        pct = 100.0 * outliers / total
        flags.append((min(50.0, pct), f"{name}: {outliers} outliers ({pct:.1f}%)"))

    return flags


def _section_min_cost(header: str, items: list[str]) -> int:
    """Tokens needed to show ``header`` plus at least one item (and a tail).

    A section is worth nothing below this cost, so priority allocation reserves
    it before handing budget to lower-priority sections.
    """
    if not items:
        return 0
    cost = _estimate_tokens(header) + _estimate_tokens(items[0])
    if len(items) > 1:
        cost += _estimate_tokens(f"... +{len(items) - 1} more")
    return cost


def _fit_section(header: str, items: list[str], budget: int) -> tuple[list[str], int]:
    """Fit as many ``items`` as ``budget`` tokens allow under ``header``.

    Returns ``(lines, tokens_used)``. Omitted items are summarized by a trailing
    ``... +N more``, whose own cost is reserved before any item is admitted, so
    the result never exceeds ``budget``. Emits nothing if the header alone
    cannot fit.
    """
    if not items:
        return [], 0

    header_cost = _estimate_tokens(header)
    if header_cost > budget:
        return [], 0

    lines = [header]
    used = header_cost
    admitted = 0

    for i, item in enumerate(items):
        remaining = len(items) - i
        item_cost = _estimate_tokens(item)
        # Reserve room for the tail we would need if this item does not fit.
        tail_cost = _estimate_tokens(f"... +{remaining} more") if remaining else 0

        if used + item_cost + (tail_cost if remaining > 1 else 0) <= budget:
            lines.append(item)
            used += item_cost
            admitted += 1
        else:
            tail = f"... +{remaining} more"
            if used + _estimate_tokens(tail) <= budget:
                lines.append(tail)
                used += _estimate_tokens(tail)
            break

    # A section header with no item beneath it spends tokens to say nothing --
    # its "+N more" tail only restates the count already in the header.
    if admitted == 0:
        return [], 0
    return lines, used


def profile_file(
    path: str | os.PathLike[str],
    *,
    engine: str = "auto",
    chunk_size: int | None = None,
    memory_limit_mb: int | None = None,
    format: str | None = None,
    max_rows: int | None = None,
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
    temporal_columns: list[str] | None = None,
) -> ProfileReport:
    """Profile a file path and return a report.

    This is the explicit file-oriented counterpart to :func:`profile`, with
    all file-only options kept in one place.

    Args:
        path: File path to profile.
        engine: Engine to use ("auto", "incremental", "columnar").
        chunk_size: Fixed chunk size for streaming (None = adaptive).
        memory_limit_mb: Memory limit in MB.
        format: Override format detection ("csv", "json", "jsonl", "parquet").
        max_rows: Maximum rows to process before stopping.
        csv_delimiter: Single-character CSV delimiter (default: comma).
        csv_flexible: Allow variable-length CSV records.
        sampling: Sampling strategy (e.g. SamplingStrategy.random(1000)).
        stop_condition: Early stop condition (e.g. StopCondition.max_rows(5000)).
            Cannot be used together with max_rows.
        on_progress: Callable receiving ProgressEvent objects during profiling.
            Only effective with engine="incremental". Files that finish faster
            than ``progress_interval_ms`` may emit only start and finish events.
        progress_interval_ms: Minimum interval between progress events in ms
            (default: 500).
        quality_dimensions: List of quality dimensions to evaluate.
            Valid values: "completeness", "consistency", "uniqueness",
            "accuracy", "timeliness", "validity", "precision". None = all
            dimensions (default). An empty list requests no dimension, which
            means quality is not analyzed at all: ``quality`` and
            ``quality_score`` are None, the same as omitting "quality" from
            ``metrics``.
        metrics: List of metric packs to compute. Valid values: "schema"
            (always included), "statistics", "patterns", "quality".
            None = all packs (default).
        locale: ISO 3166-1 alpha-2 locale for pattern detection (e.g. "IT",
            "US", "GB").
        positive_columns: Columns whose numeric values are expected to be
            non-negative.
        identifier_columns: Numeric-looking columns to treat as semantic
            identifiers instead of measures.
        temporal_columns: Columns whose values should contribute to timeliness
            quality metrics.

    Returns:
        ProfileReport with analysis results and quality metrics.
    """
    normalized_path = _normalize_existing_file(path, arg_name="path")
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
        temporal_columns=temporal_columns,
    )
    rust_report = _analyze_file(normalized_path, config)
    return ProfileReport(rust_report)


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
    temporal_columns: list[str] | None = None,
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
            Only effective with engine="incremental". Files that finish faster
            than ``progress_interval_ms`` may emit only start and finish events.
        progress_interval_ms: Minimum interval between progress events in ms
            (default: 500).
        quality_dimensions: List of quality dimensions to evaluate.
            Valid values: "completeness", "consistency", "uniqueness",
            "accuracy", "timeliness", "validity", "precision". None = all
            dimensions (default). An empty list requests no dimension, which
            means quality is not analyzed at all: ``quality`` and
            ``quality_score`` are None, the same as omitting "quality" from
            ``metrics``.
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
        temporal_columns: Columns whose values should contribute to timeliness
            quality metrics.

    Returns:
        ProfileReport with analysis results and quality metrics.
    """
    # File path — build config and delegate to Rust
    if isinstance(source, (str, pathlib.PurePath)):
        return profile_file(
            source,
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
            temporal_columns=temporal_columns,
        )

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
                temporal_columns,
            )
        ):
            return ProfilerConfig(
                max_rows=max_rows,
                quality_dimensions=quality_dimensions,
                metrics=metrics,
                locale=locale,
                positive_columns=positive_columns,
                identifier_columns=identifier_columns,
                temporal_columns=temporal_columns,
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

    def _profile_python_columns(columns: list[_Column], default_name: str) -> ProfileReport:
        rust_report = _profile_columns(columns, name or default_name, max_rows, _df_config())
        return ProfileReport(rust_report)

    if isinstance(source, dict):
        _warn_if_config_ignored()
        return _profile_python_columns(_columns_from_dict(source), "dataframe")

    if _is_list_of_dicts(source):
        _warn_if_config_ignored()
        return _profile_python_columns(_columns_from_records(source, max_rows), "dataframe")

    if isinstance(source, (bytes, bytearray, memoryview, _io.BytesIO)):
        if format is None:
            raise ValueError(
                "bytes and BytesIO sources require format='csv', 'json', 'jsonl', or 'parquet'. "
                "For async byte streams, use dataprof.asyncio.profile_bytes(data, format='csv')."
            )
        fmt = format.lower()
        buffer = _bytes_buffer(source)

        if fmt == "csv":
            columns = _columns_from_csv_bytes(buffer, csv_delimiter)
        elif fmt in ("json", "jsonl"):
            text = buffer.getvalue().decode("utf-8")
            if fmt == "jsonl":
                rows = [json.loads(line) for line in text.splitlines() if line.strip()]
            else:
                rows = json.loads(text)
            if isinstance(rows, dict):
                if all(isinstance(values, (list, tuple)) for values in rows.values()):
                    columns = _columns_from_dict(rows)
                else:
                    columns = _columns_from_records([rows], max_rows, sort_keys=True)
            elif _is_list_of_dicts(rows):
                columns = _columns_from_records(rows, max_rows, sort_keys=True)
            else:
                raise ValueError(
                    f"{fmt} bytes must decode to an object of columns or an array of "
                    f"row objects, got {type(rows).__name__}."
                )
        elif fmt == "parquet":
            # Parquet needs a columnar reader; pandas pulls in pyarrow for us.
            pd = _require_pandas("parquet byte buffers")
            return _profile_python_dataframe(pd.read_parquet(buffer), "parquet_bytes")
        else:
            raise ValueError("Unsupported bytes format. Use 'csv', 'json', 'jsonl', or 'parquet'.")

        return _profile_python_columns(columns, f"{fmt}_bytes")

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
        """Set progress callback (engine="incremental" only).

        Files that finish faster than ``progress_interval_ms`` may emit only
        start and finish events.
        """
        self._kwargs["on_progress"] = cb
        return self

    def progress_interval_ms(self, ms: int) -> Profiler:
        """Set minimum interval between progress events in ms."""
        self._kwargs["progress_interval_ms"] = ms
        return self

    def quality_dimensions(self, dims: list[str]) -> Profiler:
        """Select quality dimensions to evaluate."""
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

    def temporal_columns(self, columns: list[str]) -> Profiler:
        """Mark columns whose values should contribute to timeliness metrics."""
        self._kwargs["temporal_columns"] = columns
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
        self.unique_count_is_approximate = d.get("unique_count_is_approximate")
        self.uniqueness_ratio = d.get("uniqueness_ratio")
        self.invalid_count = d.get("invalid_count")
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

    _DEFAULT_SCORE_WEIGHTS = {
        "completeness": 0.25,
        "consistency": 0.20,
        "uniqueness": 0.15,
        "accuracy": 0.15,
        "timeliness": 0.10,
        "validity": 0.10,
        "precision": 0.05,
    }

    def __init__(self, d: dict[str, Any]):
        self._d = d
        self.low_sample_warning = bool(d.get("low_sample_warning", False))

    def _warn_flat_accessor(self, name: str) -> None:
        warnings.warn(
            f"DataQualityMetrics.{name} is deprecated; use the nested dimension "
            "properties such as completeness, consistency, uniqueness, accuracy, "
            "or timeliness instead.",
            DeprecationWarning,
            stacklevel=3,
        )

    def _dimension_value(self, dimension: str, key: str, default: Any) -> Any:
        value = self._d.get(dimension)
        if isinstance(value, dict):
            return value.get(key, default)
        return default

    @property
    def missing_values_ratio(self) -> float:
        self._warn_flat_accessor("missing_values_ratio")
        return self._dimension_value("completeness", "missing_values_ratio", 0.0)

    @property
    def complete_records_ratio(self) -> float:
        self._warn_flat_accessor("complete_records_ratio")
        return self._dimension_value("completeness", "complete_records_ratio", 100.0)

    @property
    def null_columns(self) -> list[str]:
        self._warn_flat_accessor("null_columns")
        return self._dimension_value("completeness", "null_columns", [])

    @property
    def data_type_consistency(self) -> float:
        self._warn_flat_accessor("data_type_consistency")
        return self._dimension_value("consistency", "data_type_consistency", 100.0)

    @property
    def format_violations(self) -> int:
        self._warn_flat_accessor("format_violations")
        return self._dimension_value("consistency", "format_violations", 0)

    @property
    def encoding_issues(self) -> int:
        self._warn_flat_accessor("encoding_issues")
        return self._dimension_value("consistency", "encoding_issues", 0)

    @property
    def duplicate_rows(self) -> int:
        self._warn_flat_accessor("duplicate_rows")
        return self._dimension_value("uniqueness", "duplicate_rows", 0)

    @property
    def key_uniqueness(self) -> float:
        self._warn_flat_accessor("key_uniqueness")
        return self._dimension_value("uniqueness", "key_uniqueness", 100.0)

    @property
    def high_cardinality_warning(self) -> bool:
        self._warn_flat_accessor("high_cardinality_warning")
        return self._dimension_value("uniqueness", "high_cardinality_warning", False)

    @property
    def outlier_ratio(self) -> float:
        self._warn_flat_accessor("outlier_ratio")
        return self._dimension_value("accuracy", "outlier_ratio", 0.0)

    @property
    def range_violations(self) -> int:
        self._warn_flat_accessor("range_violations")
        return self._dimension_value("accuracy", "range_violations", 0)

    @property
    def negative_values_in_positive(self) -> int:
        self._warn_flat_accessor("negative_values_in_positive")
        return self._dimension_value("accuracy", "negative_values_in_positive", 0)

    @property
    def future_dates_count(self) -> int:
        self._warn_flat_accessor("future_dates_count")
        return self._dimension_value("timeliness", "future_dates_count", 0)

    @property
    def stale_data_ratio(self) -> float:
        self._warn_flat_accessor("stale_data_ratio")
        return self._dimension_value("timeliness", "stale_data_ratio", 0.0)

    @property
    def temporal_violations(self) -> int:
        self._warn_flat_accessor("temporal_violations")
        return self._dimension_value("timeliness", "temporal_violations", 0)

    @property
    def invalid_date_values(self) -> int:
        self._warn_flat_accessor("invalid_date_values")
        return self._dimension_value("timeliness", "invalid_date_values", 0)

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

    @property
    def validity(self) -> dict[str, Any] | None:
        return self._d.get("validity")

    @property
    def precision(self) -> dict[str, Any] | None:
        return self._d.get("precision")

    @property
    def score_weights(self) -> dict[str, float]:
        weights = self._d.get("score_weights")
        if isinstance(weights, dict):
            return weights
        return self._DEFAULT_SCORE_WEIGHTS

    def overall_quality_score(self) -> float | None:
        return self._d.get("overall_score")

    def assessed_dimensions(self) -> list[str]:
        """Dimensions that had data to assess. Empty for reports serialized
        before denominators existed — no score is fabricated for them."""
        return self._d.get("assessed_dimensions") or []

    def dimension_scores(self) -> dict[str, float | None]:
        """Per-dimension scores (0-100), None when not assessable."""
        scores = self._d.get("dimension_scores")
        if isinstance(scores, dict):
            return scores
        return dict.fromkeys(
            (
                "completeness",
                "consistency",
                "uniqueness",
                "accuracy",
                "timeliness",
                "validity",
                "precision",
            )
        )


class _DictBackedReport:
    """Read-only stand-in for the native ProfileReport, built from to_dict()."""

    def __init__(self, d: dict[str, Any]):
        execution = d.get("execution") or {}
        self.source = d.get("source")
        self.source_type = d.get("source_type")
        self.engine = execution.get("engine")
        self.rows_processed = execution.get("rows_processed")
        self.columns_detected = execution.get("columns_detected")
        self.scan_time_ms = execution.get("scan_time_ms")
        self.source_exhausted = execution.get("source_exhausted")
        self.truncation_reason = execution.get("truncation_reason")
        self.bytes_consumed = execution.get("bytes_consumed")
        self.throughput_rows_sec = execution.get("throughput_rows_sec")
        self.memory_peak_mb = execution.get("memory_peak_mb")
        self.error_count = execution.get("error_count")
        # Additive field: reports written before it existed omit the key and
        # must read back as 0 (not None), matching the Rust serde default.
        self.ragged_row_count = execution.get("ragged_row_count") or 0
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
    def engine(self) -> str | None:
        return self._report.engine

    @property
    def rows(self) -> int:
        return self._report.rows_processed

    @property
    def columns(self) -> int:
        return self._report.columns_detected

    @functools.cached_property
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
            warnings.warn(
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
    def semantic_hint_bindings(self) -> list[dict[str, Any]]:
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
        statistics) to match the Rust report serialization. The document
        carries ``schema_version`` (``dataprof.REPORT_SCHEMA_VERSION``) so
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
            validity = q.validity
            if validity is not None:
                quality_dict["validity"] = validity
            precision = q.precision
            if precision is not None:
                quality_dict["precision"] = precision

        document = {
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
        }
        # Additive: only present when hints were supplied, so hint-free reports
        # keep their existing shape.
        bindings = self.semantic_hint_bindings
        if bindings:
            document["semantic_hint_bindings"] = bindings
        return document

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

    def quality_summary(self) -> dict[str, Any]:
        """Single-row quality summary for easy aggregation.

        Returns a dict with source, rows, quality_score, per-dimension scores,
        and execution_time_ms. Useful for pd.concat() across multiple reports.

        Dimension scores mirror the components of ``quality_score``: each uses
        every sub-metric of its dimension, and a dimension that had nothing to
        assess (no numeric values, no date columns, ...) is None rather than a
        vacuous 100.
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
            "validity": None,
            "precision": None,
            "execution_time_ms": self._report.scan_time_ms,
        }
        if q is not None:
            for name, score in q.dimension_scores().items():
                row[name] = _r2(score)
        return row

    def save(self, path: str | os.PathLike[str]) -> ProfileReport:
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
        elif path.endswith(".html"):
            with open(path, "w", encoding="utf-8") as f:
                f.write(self.to_html())
        elif path.endswith((".md", ".markdown")):
            with open(path, "w", encoding="utf-8") as f:
                f.write(self.to_markdown())
        else:
            raise ValueError(
                f"Unsupported format for '{path}'. Use .json, .csv, .parquet, .html, .md."
            )
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
                f"{_r2(col.null_percentage):.1f}%",
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
        qs = self.quality_score
        qs_str = f"{qs:.1f}/100" if qs is not None else "n/a"

        header = [
            f"dataset: {_one_line(self.source)} ({self.source_type})",
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

        pattern_items = [
            f"- {_one_line(col.name)}: {_pattern_cell(col)}" for col in cols if col.patterns
        ]

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

    def compare(self, other: ProfileReport) -> dict[str, Any]:
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
    def from_dict(cls, data: dict[str, Any]) -> ProfileReport:
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


asyncio = _importlib.import_module(f"{__name__}.asyncio")
