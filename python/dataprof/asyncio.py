"""Async API for dataprof.

Provides async/await versions of profiling operations. The features behind it
(``python-async``, ``async-streaming``, ``parquet-async``) are compiled into the
published wheels.

Example::

    import asyncio
    from dataprof.asyncio import profile_file

    async def main():
        report = await profile_file("data.csv")
        print(report)

    asyncio.run(main())
"""

from __future__ import annotations as _annotations

from collections.abc import Callable as _Callable
from datetime import date as _date
from pathlib import Path as _Path
from typing import Any as _Any

try:
    from ._dataprof import (  # type: ignore[import-not-found]
        infer_schema_stream_async as _infer_schema_stream_async,
        profile_bytes_async as _profile_bytes_async,
        profile_file_async as _profile_file_async,
        quick_row_count_stream_async as _quick_row_count_stream_async,
    )

    _HAS_ASYNC = True
except ImportError:
    _HAS_ASYNC = False

try:
    from ._dataprof import profile_url_async as _profile_url_async  # type: ignore[import-not-found]

    _HAS_URL = True
except ImportError:
    _HAS_URL = False

from ._arguments import _check_profile_options
from ._dataprof import (
    ProfilerConfig as _ProfilerConfig,  # type: ignore[import-not-found]
    ProgressEvent as _ProgressEvent,  # type: ignore[import-not-found]
    RowCountEstimate as _RowCountEstimate,
    SamplingStrategy as _SamplingStrategy,  # type: ignore[import-not-found]
    SchemaResult as _SchemaResult,
    StopCondition as _StopCondition,  # type: ignore[import-not-found]
)
from ._paths import _normalize_existing_file
from ._report import ProfileReport as _ProfileReport, _check_source_version

__all__ = [
    "profile_bytes",
    "profile_file",
    "profile_url",
    "infer_schema_stream",
    "quick_row_count_stream",
]


def _check_async() -> None:
    if not _HAS_ASYNC:
        raise ImportError(
            "Async support not available. Rebuild dataprof with "
            "--features 'python-async,async-streaming'."
        )


def _config_from_keywords(options: dict[str, _Any]) -> _ProfilerConfig | None:
    """Turn spelled profile keywords into a ``ProfilerConfig``.

    ``engine="auto"``, ``jsonl_on_error="skip"`` and ``None`` are the unset
    defaults, so a call that passes none of the others still builds no config.
    Unknown names never reach this function: the profile signatures list the
    keywords, and the interpreter suggests a misspelling the way ``profile()``
    does. A ``jsonl_on_error`` other than ``skip`` or ``strict`` raises the
    same ``ValueError`` as :func:`dataprof.profile_file`.
    """
    policy = options.get("jsonl_on_error", "skip")
    if policy not in ("skip", "strict"):
        raise ValueError(f"jsonl_on_error must be 'skip' or 'strict', got {policy!r}.")
    _check_profile_options(options)
    provided: dict[str, _Any] = {}
    for name, value in options.items():
        if value is None or (name == "engine" and value == "auto"):
            continue
        if name == "jsonl_on_error" and value == "skip":
            continue
        provided[name] = value
    if not provided:
        return None
    return _ProfilerConfig(**provided)


async def profile_bytes(
    data: bytes,
    *,
    format: str,
    engine: str = "auto",
    chunk_size: int | None = None,
    memory_limit_mb: int | None = None,
    max_rows: int | None = None,
    csv_delimiter: str | None = None,
    csv_flexible: bool | None = None,
    jsonl_on_error: str = "skip",
    sampling: _SamplingStrategy | None = None,
    stop_condition: _StopCondition | None = None,
    on_progress: _Callable[[_ProgressEvent], None] | None = None,
    progress_interval_ms: int | None = None,
    quality_dimensions: list[str] | None = None,
    metrics: list[str] | None = None,
    columns: list[str] | None = None,
    locale: str | None = None,
    positive_columns: list[str] | None = None,
    identifier_columns: list[str] | None = None,
    temporal_columns: list[str] | None = None,
    temporal_pairs: list[tuple[str, str]] | None = None,
    reference_date: _date | str | None = None,
    duplicate_rows_exact_limit: int | None = None,
    unique_count_exact_limits: dict[str, int] | None = None,
    unique_keys: list[list[str]] | None = None,
    unique_keys_exact_limit: int | None = None,
    source_version: str | None = None,
) -> _ProfileReport:
    """Profile in-memory bytes asynchronously.

    Args:
        data: Raw bytes to profile.
        format: Data format ("csv", "json", "jsonl", "parquet").
        engine: File profiling engine. The async stream always uses the
            incremental engine, so ``engine="columnar"`` is rejected.
        source_version: The version of the source being profiled, recorded
            verbatim as ``report.source_identity["version"]``; see
            :func:`dataprof.profile`. The other keyword arguments are the
            ones :func:`dataprof.profile_file` accepts.

    Returns:
        ProfileReport with analysis results.
    """
    _check_async()
    _check_source_version(source_version)
    config = _config_from_keywords(
        {
            "engine": engine,
            "chunk_size": chunk_size,
            "memory_limit_mb": memory_limit_mb,
            "max_rows": max_rows,
            "csv_delimiter": csv_delimiter,
            "csv_flexible": csv_flexible,
            "jsonl_on_error": jsonl_on_error,
            "sampling": sampling,
            "stop_condition": stop_condition,
            "on_progress": on_progress,
            "progress_interval_ms": progress_interval_ms,
            "quality_dimensions": quality_dimensions,
            "metrics": metrics,
            "columns": columns,
            "locale": locale,
            "positive_columns": positive_columns,
            "identifier_columns": identifier_columns,
            "temporal_columns": temporal_columns,
            "temporal_pairs": temporal_pairs,
            "reference_date": reference_date,
            "duplicate_rows_exact_limit": duplicate_rows_exact_limit,
            "unique_count_exact_limits": unique_count_exact_limits,
            "unique_keys": unique_keys,
            "unique_keys_exact_limit": unique_keys_exact_limit,
        }
    )
    if config is not None and config.engine == "columnar":
        raise ValueError(
            "engine='columnar' is not available for async bytes input; "
            "use engine='auto' or 'incremental'."
        )
    rust_report = await _profile_bytes_async(data, format, config)
    return _ProfileReport(rust_report)._record_source_version(source_version)


async def profile_file(
    path: str | _Path,
    *,
    engine: str = "auto",
    chunk_size: int | None = None,
    memory_limit_mb: int | None = None,
    format: str | None = None,
    max_rows: int | None = None,
    csv_delimiter: str | None = None,
    csv_flexible: bool | None = None,
    jsonl_on_error: str = "skip",
    sampling: _SamplingStrategy | None = None,
    stop_condition: _StopCondition | None = None,
    on_progress: _Callable[[_ProgressEvent], None] | None = None,
    progress_interval_ms: int | None = None,
    quality_dimensions: list[str] | None = None,
    metrics: list[str] | None = None,
    columns: list[str] | None = None,
    locale: str | None = None,
    positive_columns: list[str] | None = None,
    identifier_columns: list[str] | None = None,
    temporal_columns: list[str] | None = None,
    temporal_pairs: list[tuple[str, str]] | None = None,
    reference_date: _date | str | None = None,
    duplicate_rows_exact_limit: int | None = None,
    unique_count_exact_limits: dict[str, int] | None = None,
    unique_keys: list[list[str]] | None = None,
    unique_keys_exact_limit: int | None = None,
    source_version: str | None = None,
) -> _ProfileReport:
    """Profile a local file asynchronously.

    All formats supported including Parquet.

    For Parquet, ``max_rows`` selects up to 32 ranges spread across the file;
    ``report.sampled_row_ranges`` records the exact selection. Other formats
    stop at the row cap.

    Args:
        path: File path to profile.
        source_version: The version of the source being profiled, recorded
            verbatim as ``report.source_identity["version"]``; see
            :func:`dataprof.profile`. The other keyword arguments are the
            ones :func:`dataprof.profile_file` accepts.

    Returns:
        ProfileReport with analysis results.

    Raises:
        FileNotFoundError: If ``path`` does not exist.
    """
    _check_async()
    _check_source_version(source_version)
    config = _config_from_keywords(
        {
            "engine": engine,
            "chunk_size": chunk_size,
            "memory_limit_mb": memory_limit_mb,
            "format": format,
            "max_rows": max_rows,
            "csv_delimiter": csv_delimiter,
            "csv_flexible": csv_flexible,
            "jsonl_on_error": jsonl_on_error,
            "sampling": sampling,
            "stop_condition": stop_condition,
            "on_progress": on_progress,
            "progress_interval_ms": progress_interval_ms,
            "quality_dimensions": quality_dimensions,
            "metrics": metrics,
            "columns": columns,
            "locale": locale,
            "positive_columns": positive_columns,
            "identifier_columns": identifier_columns,
            "temporal_columns": temporal_columns,
            "temporal_pairs": temporal_pairs,
            "reference_date": reference_date,
            "duplicate_rows_exact_limit": duplicate_rows_exact_limit,
            "unique_count_exact_limits": unique_count_exact_limits,
            "unique_keys": unique_keys,
            "unique_keys_exact_limit": unique_keys_exact_limit,
        }
    )
    # Name a missing file the way the sync API does, not as a Rust I/O error.
    normalized_path = _normalize_existing_file(path)
    rust_report = await _profile_file_async(normalized_path, config)
    return _ProfileReport(rust_report)._record_source_version(source_version)


async def profile_url(
    url: str,
    *,
    format: str | None = None,
    engine: str = "auto",
    chunk_size: int | None = None,
    memory_limit_mb: int | None = None,
    max_rows: int | None = None,
    csv_delimiter: str | None = None,
    csv_flexible: bool | None = None,
    jsonl_on_error: str = "skip",
    sampling: _SamplingStrategy | None = None,
    stop_condition: _StopCondition | None = None,
    on_progress: _Callable[[_ProgressEvent], None] | None = None,
    progress_interval_ms: int | None = None,
    quality_dimensions: list[str] | None = None,
    metrics: list[str] | None = None,
    columns: list[str] | None = None,
    locale: str | None = None,
    positive_columns: list[str] | None = None,
    identifier_columns: list[str] | None = None,
    temporal_columns: list[str] | None = None,
    temporal_pairs: list[tuple[str, str]] | None = None,
    reference_date: _date | str | None = None,
    duplicate_rows_exact_limit: int | None = None,
    unique_count_exact_limits: dict[str, int] | None = None,
    unique_keys: list[list[str]] | None = None,
    unique_keys_exact_limit: int | None = None,
    source_version: str | None = None,
) -> _ProfileReport:
    """Profile data from a remote URL asynchronously.

    Supports CSV, JSON, and JSONL when async streaming is compiled in.
    Remote Parquet uses HTTP Range requests and additionally requires the
    ``parquet-async`` feature.

    For Parquet, ``max_rows`` selects up to 32 ranges spread across the source;
    ``report.sampled_row_ranges`` records the exact selection. Other formats
    stop at the row cap.

    Args:
        url: URL to profile.
        format: Override format detection (optional).
        source_version: The version of the source being profiled, recorded
            verbatim as ``report.source_identity["version"]``; see
            :func:`dataprof.profile`. The other keyword arguments are the
            ones :func:`dataprof.profile_file` accepts.

    Returns:
        ProfileReport with analysis results.
    """
    if not _HAS_URL:
        raise ImportError(
            "URL profiling not available. Rebuild dataprof with "
            "--features 'python-async,async-streaming'. Remote Parquet "
            "also requires 'parquet-async'."
        )
    _check_source_version(source_version)
    config = _config_from_keywords(
        {
            "engine": engine,
            "chunk_size": chunk_size,
            "memory_limit_mb": memory_limit_mb,
            "max_rows": max_rows,
            "csv_delimiter": csv_delimiter,
            "csv_flexible": csv_flexible,
            "jsonl_on_error": jsonl_on_error,
            "sampling": sampling,
            "stop_condition": stop_condition,
            "on_progress": on_progress,
            "progress_interval_ms": progress_interval_ms,
            "quality_dimensions": quality_dimensions,
            "metrics": metrics,
            "columns": columns,
            "locale": locale,
            "positive_columns": positive_columns,
            "identifier_columns": identifier_columns,
            "temporal_columns": temporal_columns,
            "temporal_pairs": temporal_pairs,
            "reference_date": reference_date,
            "duplicate_rows_exact_limit": duplicate_rows_exact_limit,
            "unique_count_exact_limits": unique_count_exact_limits,
            "unique_keys": unique_keys,
            "unique_keys_exact_limit": unique_keys_exact_limit,
        }
    )
    rust_report = await _profile_url_async(url, format, config)
    return _ProfileReport(rust_report)._record_source_version(source_version)


async def infer_schema_stream(
    data: bytes,
    *,
    format: str,
) -> _SchemaResult:
    """Infer schema from in-memory bytes asynchronously.

    Reads only a small sample. Parquet is not supported.

    Args:
        data: Raw bytes to analyze.
        format: Data format ("csv", "json", "jsonl").

    Returns:
        SchemaResult with column names and types.
    """
    _check_async()
    return await _infer_schema_stream_async(data, format)


async def quick_row_count_stream(
    data: bytes,
    *,
    format: str,
) -> _RowCountEstimate:
    """Quick row count from in-memory bytes asynchronously.

    Always a full scan. Parquet not supported.

    Args:
        data: Raw bytes to count.
        format: Data format ("csv", "json", "jsonl").

    Returns:
        RowCountEstimate with the count.
    """
    _check_async()
    return await _quick_row_count_stream_async(data, format)
