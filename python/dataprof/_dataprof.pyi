"""Type stubs for the compiled Rust extension module `dataprof._dataprof`.

This declares the raw symbols exported by the PyO3 extension. The public
`dataprof` package (`__init__.pyi`) re-exports the data types from here and
wraps the module-level functions with Python-friendly APIs.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

__version__: str
_compiled_capabilities: dict[str, bool]

# --- Configuration ---

class ProfilerConfig:
    """Profiler configuration with Python-friendly kwargs."""

    def __init__(
        self,
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
    ) -> None: ...
    @property
    def engine(self) -> str: ...
    @property
    def chunk_size(self) -> int | None: ...
    @property
    def memory_limit_mb(self) -> int | None: ...
    @property
    def format(self) -> str | None: ...
    @property
    def max_rows(self) -> int | None: ...
    @property
    def locale(self) -> str | None: ...
    @property
    def positive_columns(self) -> list[str]: ...
    @property
    def identifier_columns(self) -> list[str]: ...
    @property
    def temporal_columns(self) -> list[str]: ...

# --- Sampling ---

class SamplingStrategy:
    """Sampling strategy for controlling how data is sampled during profiling."""

    @staticmethod
    def none() -> SamplingStrategy: ...
    @staticmethod
    def random(size: int) -> SamplingStrategy: ...
    @staticmethod
    def reservoir(size: int) -> SamplingStrategy: ...
    @staticmethod
    def stratified(key_columns: list[str], samples_per_stratum: int) -> SamplingStrategy: ...
    @staticmethod
    def progressive(
        initial_size: int,
        confidence_level: float = 0.95,
        max_size: int = 100_000,
    ) -> SamplingStrategy: ...
    @staticmethod
    def systematic(interval: int) -> SamplingStrategy: ...
    @staticmethod
    def importance(weight_threshold: float) -> SamplingStrategy: ...
    @staticmethod
    def multi_stage(stages: list[SamplingStrategy]) -> SamplingStrategy: ...
    @staticmethod
    def adaptive(total_rows: int | None = None, file_size_mb: float = 0.0) -> SamplingStrategy: ...

# --- Stop Conditions ---

class StopCondition:
    """Composable stop condition for early termination."""

    @staticmethod
    def max_rows(n: int) -> StopCondition: ...
    @staticmethod
    def max_bytes(n: int) -> StopCondition: ...
    @staticmethod
    def schema_stable(consecutive_stable_rows: int) -> StopCondition: ...
    @staticmethod
    def confidence_threshold(threshold: float) -> StopCondition: ...
    @staticmethod
    def memory_pressure(threshold: float) -> StopCondition: ...
    @staticmethod
    def never() -> StopCondition: ...
    @staticmethod
    def schema_inference() -> StopCondition: ...
    @staticmethod
    def quality_sample() -> StopCondition: ...
    def __or__(self, other: StopCondition) -> StopCondition: ...
    def __and__(self, other: StopCondition) -> StopCondition: ...

# --- Progress ---

class ProgressEvent:
    """A progress event emitted during profiling."""

    kind: str
    rows_processed: int | None
    bytes_consumed: int | None
    elapsed_ms: int | None
    processing_speed: float | None
    percentage: float | None
    column_names: list[str] | None
    total_rows: int | None
    total_bytes: int | None
    truncated: bool | None
    message: str | None
    estimated_total_rows: int | None
    estimated_total_bytes: int | None

# --- Data types ---

class Pattern:
    """Detected pattern statistics."""

    name: str
    regex: str
    match_count: int
    match_percentage: float
    category: str
    confidence: float

class ColumnProfile:
    """Column-level profiling statistics."""

    name: str
    data_type: str
    total_count: int
    null_count: int
    unique_count: int | None
    unique_count_is_approximate: bool | None
    invalid_count: int | None
    null_percentage: float
    uniqueness_ratio: float
    min: float | None
    max: float | None
    mean: float | None
    std_dev: float | None
    variance: float | None
    median: float | None
    mode: float | None
    skewness: float | None
    kurtosis: float | None
    coefficient_of_variation: float | None
    quartiles: dict[str, float] | None
    is_approximate: bool | None
    outlier_count: int | None
    min_length: int | None
    max_length: int | None
    avg_length: float | None
    true_count: int | None
    false_count: int | None
    true_ratio: float | None
    #: Detected patterns, or ``None`` when no pattern evidence is available for
    #: this column -- detection did not run (no "patterns" metric pack), or the
    #: payload it was rebuilt from omitted the key. ``[]`` means detection ran
    #: and matched nothing. Redaction gates must treat ``None`` as unknown
    #: rather than safe; ``[]`` is a positive all-clear.
    patterns: list[Pattern] | None

class DataQualityMetrics:
    """Quality metrics informed by ISO 8000/25012 concepts."""

    missing_values_ratio: float
    complete_records_ratio: float
    null_columns: list[str]
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
    low_sample_warning: bool
    score_weights: dict[str, float]

    @property
    def completeness(self) -> dict[str, Any] | None: ...
    @property
    def consistency(self) -> dict[str, Any] | None: ...
    @property
    def uniqueness(self) -> dict[str, Any] | None: ...
    @property
    def accuracy(self) -> dict[str, Any] | None: ...
    @property
    def timeliness(self) -> dict[str, Any] | None: ...
    @property
    def validity(self) -> dict[str, Any] | None: ...
    @property
    def precision(self) -> dict[str, Any] | None: ...
    def overall_quality_score(self) -> float: ...
    def assessed_dimensions(self) -> list[str]: ...
    def dimension_scores(self) -> dict[str, float | None]: ...

# --- Partial analysis results ---

class SchemaResult:
    """Result of fast schema inference."""

    @property
    def columns(self) -> list[dict[str, str]]: ...
    @property
    def rows_sampled(self) -> int: ...
    @property
    def inference_time_ms(self) -> int: ...
    @property
    def schema_stable(self) -> bool: ...
    @property
    def num_columns(self) -> int: ...
    @property
    def column_names(self) -> list[str]: ...

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

class StructureColumnSummary:
    """Structural summary for one column."""

    @property
    def name(self) -> str: ...
    @property
    def data_type(self) -> str: ...
    @property
    def total_count(self) -> int | None: ...
    @property
    def null_count(self) -> int | None: ...
    @property
    def null_ratio(self) -> float | None: ...
    @property
    def unique_count(self) -> int | None: ...
    @property
    def uniqueness_ratio(self) -> float | None: ...
    @property
    def distinct_count_approximate(self) -> bool | None: ...
    @property
    def provenance(self) -> str: ...

class StructureReport:
    """Lightweight structural summary for a file."""

    @property
    def source(self) -> str: ...
    @property
    def format(self) -> str: ...
    @property
    def row_count(self) -> RowCountEstimate: ...
    @property
    def rows_sampled(self) -> int: ...
    @property
    def source_exhausted(self) -> bool: ...
    @property
    def truncated(self) -> bool: ...
    @property
    def truncation_reason(self) -> str | None: ...
    @property
    def delimiter(self) -> str | None: ...
    @property
    def columns(self) -> list[StructureColumnSummary]: ...
    @property
    def warnings(self) -> list[str]: ...

# --- Arrow interop ---

class RecordBatch:
    """Arrow RecordBatch with PyCapsule interface for zero-copy exchange."""

    @property
    def num_rows(self) -> int: ...
    @property
    def num_columns(self) -> int: ...
    @property
    def column_names(self) -> list[str]: ...
    def to_pandas(self) -> Any: ...
    def to_polars(self) -> Any: ...
    def __arrow_c_schema__(self) -> object: ...
    def __arrow_c_array__(
        self, requested_schema: object | None = None
    ) -> tuple[object, object]: ...

# --- Raw report ---

class ProfileReport:
    """Raw Rust profiling report (wrapped by the public `dataprof.ProfileReport`)."""

    @property
    def source(self) -> str: ...
    @property
    def source_type(self) -> str: ...
    @property
    def source_library(self) -> str | None: ...
    @property
    def memory_bytes(self) -> int | None: ...
    @property
    def engine(self) -> str | None: ...
    @property
    def rows_processed(self) -> int: ...
    @property
    def columns_detected(self) -> int: ...
    @property
    def scan_time_ms(self) -> int: ...
    @property
    def source_exhausted(self) -> bool: ...
    @property
    def truncation_reason(self) -> str | None: ...
    @property
    def bytes_consumed(self) -> int | None: ...
    @property
    def throughput_rows_sec(self) -> float | None: ...
    @property
    def memory_peak_mb(self) -> float | None: ...
    @property
    def error_count(self) -> int: ...
    @property
    def sampling_applied(self) -> bool: ...
    @property
    def sampling_ratio(self) -> float | None: ...
    @property
    def column_profiles(self) -> list[ColumnProfile]: ...
    @property
    def quality(self) -> DataQualityMetrics | None: ...
    @property
    def quality_score(self) -> float | None: ...
    def to_json(self) -> str: ...

# --- Module-level functions ---

def analyze_file(path: str, config: ProfilerConfig | None) -> ProfileReport: ...
def list_patterns(locale: str | None = None) -> list[dict[str, Any]]: ...
def profile_dataframe(
    df: Any, name: str, max_rows: int | None, config: ProfilerConfig | None
) -> ProfileReport: ...
def profile_arrow(
    source: Any, name: str, max_rows: int | None, config: ProfilerConfig | None
) -> ProfileReport: ...
def profile_columns(
    columns: list[tuple[str, list[str | None]]],
    name: str,
    max_rows: int | None,
    config: ProfilerConfig | None,
) -> ProfileReport: ...
def infer_schema(path: str) -> SchemaResult: ...
def quick_row_count(path: str) -> RowCountEstimate: ...
def analyze_structure(path: str, max_rows: int | None = None) -> StructureReport: ...
def analyze_csv_to_arrow(path: str) -> RecordBatch: ...
def analyze_parquet_to_arrow(path: str) -> RecordBatch: ...

# Async functions (available only with the `python-async` feature build)
async def profile_file_async(path: str, config: ProfilerConfig | None) -> ProfileReport: ...
async def profile_bytes_async(
    data: bytes, format: str, config: ProfilerConfig | None
) -> ProfileReport: ...
async def profile_url_async(
    url: str, format: str | None, config: ProfilerConfig | None
) -> ProfileReport: ...
async def infer_schema_stream_async(data: bytes, format: str) -> SchemaResult: ...
async def quick_row_count_stream_async(data: bytes, format: str) -> RowCountEstimate: ...

# Database functions (available only with the `database` feature build)
async def analyze_database_async(
    connection_string: str,
    query: str,
    batch_size: int = 10000,
    calculate_quality: bool = False,
) -> ProfileReport: ...
async def count_table_rows_async(connection_string: str, table_name: str) -> int: ...
async def get_table_schema_async(connection_string: str, table_name: str) -> list[str]: ...
async def test_connection_async(connection_string: str) -> bool: ...
