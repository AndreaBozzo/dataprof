"""Type stubs for dataprof.interop — low-level Rust bindings."""

from dataprof import (
    ColumnProfile,
    DataQualityMetrics,
    ProfilerConfig,
    ProfileReport,
    RecordBatch,
)

def analyze_file(path: str, config: ProfilerConfig | None = None) -> ProfileReport:
    """Analyze a file with optional config. Format auto-detected from extension."""
    ...

def profile_dataframe(df: object, name: str = "dataframe") -> ProfileReport:
    """Profile a pandas/polars DataFrame via Arrow PyCapsule protocol."""
    ...

def profile_arrow(table: object, name: str = "arrow_table") -> ProfileReport:
    """Profile a PyArrow Table or RecordBatch directly."""
    ...

def analyze_csv_to_arrow(path: str) -> RecordBatch:
    """Analyze CSV and return column stats as Arrow RecordBatch."""
    ...

def analyze_parquet_to_arrow(path: str) -> RecordBatch:
    """Analyze Parquet and return column stats as Arrow RecordBatch."""
    ...

__all__ = [
    "analyze_file",
    "profile_dataframe",
    "profile_arrow",
    "analyze_csv_to_arrow",
    "analyze_parquet_to_arrow",
    "ProfilerConfig",
    "ProfileReport",
    "ColumnProfile",
    "DataQualityMetrics",
    "RecordBatch",
]
