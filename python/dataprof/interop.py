"""Low-level interop functions for advanced usage.

These are direct bindings to the Rust profiling engine, without the
high-level dispatch and wrapping provided by ``dataprof.profile()``.
"""

from __future__ import annotations

import os

from dataprof import column_to_dict
from dataprof._dataprof import (  # type: ignore[import-not-found]
    ColumnProfile,
    DataQualityMetrics,
    ProfilerConfig,
    ProfileReport,
    RecordBatch,
    analyze_csv_to_arrow as _analyze_csv_to_arrow,
    analyze_file as _analyze_file,
    analyze_parquet_to_arrow as _analyze_parquet_to_arrow,
    profile_arrow,
    profile_dataframe,
)


def _normalize_pathlike(path: str | os.PathLike[str], *, arg_name: str = "path") -> str:
    if isinstance(path, str):
        return path
    if isinstance(path, os.PathLike):
        normalized = os.fspath(path)
        if isinstance(normalized, str):
            return normalized
    raise TypeError(f"Expected str or path-like object for {arg_name}, got {type(path).__name__}")


def analyze_file(
    path: str | os.PathLike[str],
    config: ProfilerConfig | None = None,
) -> ProfileReport:
    return _analyze_file(_normalize_pathlike(path), config)


def analyze_csv_to_arrow(path: str | os.PathLike[str]) -> RecordBatch:
    return _analyze_csv_to_arrow(_normalize_pathlike(path))


def analyze_parquet_to_arrow(path: str | os.PathLike[str]) -> RecordBatch:
    return _analyze_parquet_to_arrow(_normalize_pathlike(path))


__all__ = [
    "analyze_file",
    "profile_dataframe",
    "profile_arrow",
    "analyze_csv_to_arrow",
    "analyze_parquet_to_arrow",
    "column_to_dict",
    "ProfilerConfig",
    "ProfileReport",
    "ColumnProfile",
    "DataQualityMetrics",
    "RecordBatch",
]
