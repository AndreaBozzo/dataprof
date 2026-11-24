"""Type stubs for dataprof.asyncio."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from pathlib import Path

from ._dataprof import (
    ProgressEvent,
    RowCountEstimate,
    SamplingStrategy,
    SchemaResult,
    StopCondition,
)
from ._report import ProfileReport

__all__ = [
    "profile_bytes",
    "profile_file",
    "profile_url",
    "infer_schema_stream",
    "quick_row_count_stream",
]

# True when the extension was built with the corresponding feature flags.
_HAS_ASYNC: bool
_HAS_URL: bool

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
    sampling: SamplingStrategy | None = None,
    stop_condition: StopCondition | None = None,
    on_progress: Callable[[ProgressEvent], None] | None = None,
    progress_interval_ms: int | None = None,
    quality_dimensions: list[str] | None = None,
    metrics: list[str] | None = None,
    columns: list[str] | None = None,
    locale: str | None = None,
    positive_columns: list[str] | None = None,
    identifier_columns: list[str] | None = None,
    temporal_columns: list[str] | None = None,
    temporal_pairs: list[tuple[str, str]] | None = None,
    reference_date: date | str | None = None,
    duplicate_rows_exact_limit: int | None = None,
    unique_count_exact_limits: dict[str, int] | None = None,
    source_version: str | None = None,
) -> ProfileReport: ...
async def profile_file(
    path: str | Path,
    *,
    engine: str = "auto",
    chunk_size: int | None = None,
    memory_limit_mb: int | None = None,
    format: str | None = None,
    max_rows: int | None = None,
    csv_delimiter: str | None = None,
    csv_flexible: bool | None = None,
    jsonl_on_error: str = "skip",
    sampling: SamplingStrategy | None = None,
    stop_condition: StopCondition | None = None,
    on_progress: Callable[[ProgressEvent], None] | None = None,
    progress_interval_ms: int | None = None,
    quality_dimensions: list[str] | None = None,
    metrics: list[str] | None = None,
    columns: list[str] | None = None,
    locale: str | None = None,
    positive_columns: list[str] | None = None,
    identifier_columns: list[str] | None = None,
    temporal_columns: list[str] | None = None,
    temporal_pairs: list[tuple[str, str]] | None = None,
    reference_date: date | str | None = None,
    duplicate_rows_exact_limit: int | None = None,
    unique_count_exact_limits: dict[str, int] | None = None,
    source_version: str | None = None,
) -> ProfileReport: ...
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
    sampling: SamplingStrategy | None = None,
    stop_condition: StopCondition | None = None,
    on_progress: Callable[[ProgressEvent], None] | None = None,
    progress_interval_ms: int | None = None,
    quality_dimensions: list[str] | None = None,
    metrics: list[str] | None = None,
    columns: list[str] | None = None,
    locale: str | None = None,
    positive_columns: list[str] | None = None,
    identifier_columns: list[str] | None = None,
    temporal_columns: list[str] | None = None,
    temporal_pairs: list[tuple[str, str]] | None = None,
    reference_date: date | str | None = None,
    duplicate_rows_exact_limit: int | None = None,
    unique_count_exact_limits: dict[str, int] | None = None,
    source_version: str | None = None,
) -> ProfileReport: ...
async def infer_schema_stream(
    data: bytes,
    *,
    format: str,
) -> SchemaResult: ...
async def quick_row_count_stream(
    data: bytes,
    *,
    format: str,
) -> RowCountEstimate: ...
