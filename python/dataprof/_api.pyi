"""Type stubs for _api."""

from __future__ import annotations

from collections.abc import Callable
from os import PathLike
from typing import Any

from ._dataprof import (
    ProgressEvent,
    RowCountEstimate,
    SamplingStrategy,
    SchemaResult,
    StopCondition,
    StructureReport,
)
from ._report import ProfileReport

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
) -> ProfileReport:
    """Profile a data source (file path, DataFrame, or Arrow object)."""
    ...

def profile_file(
    path: str | PathLike[str],
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
) -> ProfileReport:
    """Profile a file path with file-oriented options."""
    ...

def infer_schema(path: str | PathLike[str]) -> SchemaResult:
    """Infer the schema of a file from a string or path-like object."""
    ...

def quick_row_count(path: str | PathLike[str]) -> RowCountEstimate:
    """Estimate or count rows from a string or path-like object."""
    ...

def analyze_structure(path: str | PathLike[str], max_rows: int | None = None) -> StructureReport:
    """Analyze file structure with a bounded, lightweight pass."""
    ...

def list_patterns(locale: str | None = None) -> list[dict[str, Any]]:
    """List supported pattern detectors."""
    ...
