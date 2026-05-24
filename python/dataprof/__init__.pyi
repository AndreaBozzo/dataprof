"""Type stubs for dataprof."""

from __future__ import annotations

from os import PathLike
from typing import Any, Callable, Iterator

# Version
__version__: str

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
    """Composable stop condition for early termination.

    Combine with ``|`` (any) or ``&`` (all) operators::

        stop = StopCondition.max_rows(10000) | StopCondition.max_bytes(50_000_000)
    """

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

# --- Primary API ---

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
    """Profile a data source (file path, DataFrame, or Arrow object)."""
    ...

def infer_schema(path: str | PathLike[str]) -> SchemaResult:
    """Infer the schema of a file from a string or path-like object."""
    ...

def quick_row_count(path: str | PathLike[str]) -> RowCountEstimate:
    """Estimate or count rows from a string or path-like object."""
    ...

def column_to_dict(col: ColumnProfile) -> dict[str, Any]:
    """Convert a ColumnProfile to the nested dict layout used in ``report.to_dict()['columns']``."""
    ...

class Profiler:
    """Builder-style profiler configuration.

    Chainable methods accumulate settings; call ``.profile(source)`` to run.

    Example::

        report = dp.Profiler().engine("incremental").max_rows(5000).profile("data.csv")
    """

    def __init__(self) -> None: ...
    def engine(self, engine: str) -> Profiler: ...
    def chunk_size(self, n: int) -> Profiler: ...
    def memory_limit_mb(self, mb: int) -> Profiler: ...
    def format(self, fmt: str) -> Profiler: ...
    def max_rows(self, n: int) -> Profiler: ...
    def name(self, name: str) -> Profiler: ...
    def csv_delimiter(self, d: str) -> Profiler: ...
    def csv_flexible(self, flexible: bool) -> Profiler: ...
    def sampling(self, strategy: SamplingStrategy) -> Profiler: ...
    def stop_condition(self, cond: StopCondition) -> Profiler: ...
    def on_progress(self, cb: Callable[[ProgressEvent], None]) -> Profiler: ...
    def progress_interval_ms(self, ms: int) -> Profiler: ...
    def quality_dimensions(self, dims: list[str]) -> Profiler: ...
    def stop_when(self, condition: StopCondition | str) -> Profiler: ...
    def metrics(self, packs: list[str]) -> Profiler: ...
    def locale(self, locale: str) -> Profiler: ...
    def positive_columns(self, columns: list[str]) -> Profiler: ...
    def identifier_columns(self, columns: list[str]) -> Profiler: ...
    def profile(self, source: Any) -> ProfileReport: ...

# --- Result Types ---

class ProfileReport:
    """High-level profiling report with export methods.

    Supports dict-like column access::

        report["column_name"]          # -> ColumnProfile
        "column_name" in report        # -> bool
        for name in report: ...        # iterate column names
        len(report)                    # number of columns
    """

    @property
    def source(self) -> str: ...
    @property
    def source_type(self) -> str: ...
    @property
    def rows(self) -> int: ...
    @property
    def columns(self) -> int: ...
    @property
    def column_profiles(self) -> dict[str, ColumnProfile]: ...
    @property
    def quality_score(self) -> float | None: ...
    @property
    def quality(self) -> DataQualityMetrics | None: ...
    @property
    def low_sample_warning(self) -> bool: ...
    @property
    def execution_time_ms(self) -> int: ...
    @property
    def throughput(self) -> float | None: ...
    @property
    def memory_peak_mb(self) -> float | None: ...
    @property
    def truncation_reason(self) -> str | None: ...
    @property
    def source_exhausted(self) -> bool: ...
    @property
    def sampling_applied(self) -> bool: ...
    @property
    def sampling_ratio(self) -> float | None: ...

    # Mapping protocol
    def __getitem__(self, key: str) -> ColumnProfile: ...
    def __contains__(self, key: object) -> bool: ...
    def __iter__(self) -> Iterator[str]: ...
    def __len__(self) -> int: ...

    # Export methods
    def to_dict(self) -> dict[str, Any]: ...
    def to_json(self, indent: int = 2) -> str: ...
    def to_dataframe(self) -> Any:
        """Column profiles as pandas DataFrame (all stats, rounded). Requires pandas."""
        ...
    def to_polars(self) -> Any:
        """Column profiles as polars DataFrame (all stats, rounded). Requires polars."""
        ...
    def to_arrow(self) -> Any:
        """Column profiles as PyArrow Table (all stats, rounded). Requires pyarrow."""
        ...
    def describe(self) -> Any:
        """Transposed summary (like pandas describe). Returns pandas DataFrame or dict."""
        ...
    def quality_summary(self) -> dict[str, Any]:
        """Single-row quality summary dict for easy aggregation."""
        ...
    def save(self, path: str) -> ProfileReport:
        """Save report to file (.json, .csv, or .parquet)."""
        ...

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
    patterns: list[Pattern] | None

class DataQualityMetrics:
    """ISO 8000/25012 data quality metrics.

    Flat accessors return defaults (0.0 / 0 / [] / False) for dimensions
    not computed. Use the nested dimension properties to check which
    dimensions were actually evaluated.
    """

    # Flat backward-compatible accessors
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

    # Nested dimension accessors (None when dimension was not requested)
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
    def overall_quality_score(self) -> float: ...

# --- Partial Analysis ---

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

# --- Database (async) ---

async def analyze_database_async(
    connection_string: str,
    query: str,
    batch_size: int = 10000,
    calculate_quality: bool = False,
) -> ProfileReport:
    """Analyze a database query asynchronously. Returns a ProfileReport."""
    ...

async def test_connection_async(connection_string: str) -> bool:
    """Test an async database connection. Returns True if successful."""
    ...

async def get_table_schema_async(connection_string: str, table_name: str) -> list[str]:
    """Get column names for a table asynchronously."""
    ...

async def count_table_rows_async(connection_string: str, table_name: str) -> int:
    """Count rows in a table asynchronously."""
    ...

# --- Arrow Interop ---

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

# --- Exports ---

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
    "SchemaResult",
    "RowCountEstimate",
    "RecordBatch",
    "__version__",
]
