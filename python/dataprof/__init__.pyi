"""Type stubs for dataprof."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Union

# Version
__version__: str

# --- Configuration ---

class ProfilerConfig:
    """Profiler configuration with Python-friendly kwargs."""

    def __init__(
        self,
        engine: str = "auto",
        chunk_size: Optional[int] = None,
        memory_limit_mb: Optional[int] = None,
        format: Optional[str] = None,
        max_rows: Optional[int] = None,
        csv_delimiter: Optional[str] = None,
        csv_flexible: Optional[bool] = None,
    ) -> None: ...

    @property
    def engine(self) -> str: ...
    @property
    def chunk_size(self) -> Optional[int]: ...
    @property
    def memory_limit_mb(self) -> Optional[int]: ...
    @property
    def format(self) -> Optional[str]: ...
    @property
    def max_rows(self) -> Optional[int]: ...

# --- Primary API ---

def profile(
    source: Any,
    *,
    engine: str = "auto",
    chunk_size: Optional[int] = None,
    memory_limit_mb: Optional[int] = None,
    format: Optional[str] = None,
    max_rows: Optional[int] = None,
    name: Optional[str] = None,
    csv_delimiter: Optional[str] = None,
    csv_flexible: Optional[bool] = None,
) -> ProfileReport:
    """Profile a data source (file path, DataFrame, or Arrow object)."""
    ...

# --- Result Types ---

class ProfileReport:
    """High-level profiling report with export methods."""

    @property
    def source(self) -> str: ...
    @property
    def source_type(self) -> str: ...
    @property
    def rows(self) -> int: ...
    @property
    def columns(self) -> int: ...
    @property
    def column_profiles(self) -> List[ColumnProfile]: ...
    @property
    def quality_score(self) -> Optional[float]: ...
    @property
    def quality(self) -> Optional[DataQualityMetrics]: ...
    @property
    def execution_time_ms(self) -> int: ...
    @property
    def throughput(self) -> Optional[float]: ...
    @property
    def memory_peak_mb(self) -> Optional[float]: ...
    @property
    def truncation_reason(self) -> Optional[str]: ...
    @property
    def source_exhausted(self) -> bool: ...
    @property
    def sampling_applied(self) -> bool: ...
    @property
    def sampling_ratio(self) -> Optional[float]: ...

    def to_dict(self) -> Dict[str, Any]: ...
    def to_json(self, indent: int = 2) -> str: ...
    def to_dataframe(self) -> Any:
        """Returns pandas.DataFrame. Requires pandas."""
        ...
    def save(self, path: str) -> ProfileReport: ...

class ColumnProfile:
    """Column-level profiling statistics."""

    name: str
    data_type: str
    total_count: int
    null_count: int
    unique_count: Optional[int]
    null_percentage: float
    uniqueness_ratio: float
    min: Optional[float]
    max: Optional[float]
    mean: Optional[float]
    std_dev: Optional[float]
    variance: Optional[float]
    median: Optional[float]
    mode: Optional[float]
    skewness: Optional[float]
    kurtosis: Optional[float]
    coefficient_of_variation: Optional[float]
    quartiles: Optional[Dict[str, float]]
    is_approximate: Optional[bool]

class DataQualityMetrics:
    """ISO 8000/25012 data quality metrics."""

    missing_values_ratio: float
    complete_records_ratio: float
    null_columns: List[str]
    data_type_consistency: float
    format_violations: int
    encoding_issues: int
    duplicate_rows: int
    key_uniqueness: float
    high_cardinality_warning: bool
    outlier_ratio: float
    range_violations: int
    negative_values_in_positive: int
    future_dates_count: int
    stale_data_ratio: float
    temporal_violations: int

    def overall_quality_score(self) -> float: ...

# --- Partial Analysis ---

class SchemaResult:
    """Result of fast schema inference."""

    @property
    def columns(self) -> List[Dict[str, str]]: ...
    @property
    def rows_sampled(self) -> int: ...
    @property
    def inference_time_ms(self) -> int: ...
    @property
    def schema_stable(self) -> bool: ...
    @property
    def num_columns(self) -> int: ...
    @property
    def column_names(self) -> List[str]: ...

class RowCountEstimate:
    """Result of a quick row count."""

    @property
    def count(self) -> int: ...
    @property
    def exact(self) -> bool: ...
    @property
    def method(self) -> str: ...
    @property
    def count_time_ms(self) -> int: ...

def infer_schema(path: str) -> SchemaResult:
    """Infer the schema of a file (fast, reads only a small sample)."""
    ...

def quick_row_count(path: str) -> RowCountEstimate:
    """Quick row count (exact for small files/Parquet, estimated for large files)."""
    ...

# --- Arrow Interop ---

class RecordBatch:
    """Arrow RecordBatch with PyCapsule interface for zero-copy exchange."""

    @property
    def num_rows(self) -> int: ...
    @property
    def num_columns(self) -> int: ...
    @property
    def column_names(self) -> List[str]: ...

    def to_pandas(self) -> Any: ...
    def to_polars(self) -> Any: ...
    def __arrow_c_schema__(self) -> object: ...
    def __arrow_c_array__(
        self, requested_schema: Optional[object] = None
    ) -> Tuple[object, object]: ...

# --- Exports ---

__all__ = [
    "profile",
    "ProfileReport",
    "ProfilerConfig",
    "infer_schema",
    "quick_row_count",
    "SchemaResult",
    "RowCountEstimate",
    "RecordBatch",
    "__version__",
]
