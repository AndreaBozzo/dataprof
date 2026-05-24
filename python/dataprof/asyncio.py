"""Async API for dataprof.

Provides async/await versions of profiling operations. Requires the
``python-async`` and ``async-streaming`` features to be compiled.

Example::

    import asyncio
    from dataprof.asyncio import profile_file

    async def main():
        report = await profile_file("data.csv")
        print(report)

    asyncio.run(main())
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

try:
    from ._dataprof import (
        infer_schema_stream_async as _infer_schema_stream_async,
    )
    from ._dataprof import (  # type: ignore[import-not-found]
        profile_bytes_async as _profile_bytes_async,
    )
    from ._dataprof import (
        profile_file_async as _profile_file_async,
    )
    from ._dataprof import (
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

from . import ProfileReport, RowCountEstimate, SchemaResult
from ._dataprof import ProfilerConfig  # type: ignore[import-not-found]


def _check_async() -> None:
    if not _HAS_ASYNC:
        raise ImportError(
            "Async support not available. Rebuild dataprof with "
            "--features 'python-async,async-streaming'."
        )


async def profile_bytes(
    data: bytes,
    *,
    format: str,
    **kwargs: Any,
) -> ProfileReport:
    """Profile in-memory bytes asynchronously.

    Args:
        data: Raw bytes to profile.
        format: Data format ("csv", "json", "jsonl").
        **kwargs: Additional config options (passed to ProfilerConfig).

    Returns:
        ProfileReport with analysis results.
    """
    _check_async()
    config = ProfilerConfig(**kwargs) if kwargs else None
    rust_report = await _profile_bytes_async(data, format, config)
    return ProfileReport(rust_report)


async def profile_file(
    path: str | Path,
    **kwargs: Any,
) -> ProfileReport:
    """Profile a local file asynchronously.

    All formats supported including Parquet.

    Args:
        path: File path to profile.
        **kwargs: Additional config options (passed to ProfilerConfig).

    Returns:
        ProfileReport with analysis results.
    """
    _check_async()
    config = ProfilerConfig(**kwargs) if kwargs else None
    rust_report = await _profile_file_async(str(path), config)
    return ProfileReport(rust_report)


async def profile_url(
    url: str,
    *,
    format: str | None = None,
    **kwargs: Any,
) -> ProfileReport:
    """Profile data from a remote URL asynchronously.

    Supports CSV, JSON, and JSONL when async streaming is compiled in.
    Remote Parquet uses HTTP Range requests and additionally requires the
    ``parquet-async`` feature.

    Args:
        url: URL to profile.
        format: Override format detection (optional).
        **kwargs: Additional config options (passed to ProfilerConfig).

    Returns:
        ProfileReport with analysis results.
    """
    if not _HAS_URL:
        raise ImportError(
            "URL profiling not available. Rebuild dataprof with "
            "--features 'python-async,async-streaming'. Remote Parquet "
            "also requires 'parquet-async'."
        )
    config = ProfilerConfig(**kwargs) if kwargs else None
    rust_report = await _profile_url_async(url, format, config)
    return ProfileReport(rust_report)


async def infer_schema_stream(
    data: bytes,
    *,
    format: str,
) -> SchemaResult:
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
) -> RowCountEstimate:
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
