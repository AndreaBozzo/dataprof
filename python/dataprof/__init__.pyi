"""
Type stubs for dataprof Python bindings.

This module provides data profiling and quality assessment functionality
implemented in Rust with Python bindings via PyO3.
"""

from typing import Any, Dict, List, Optional
from typing_extensions import Self

try:
    import pandas as pd
    _PANDAS_AVAILABLE = True
except ImportError:
    _PANDAS_AVAILABLE = False

# Version is imported from Rust binary module (_dataprof)
__version__: str

# Core analysis functions
def analyze_csv_file(path: str) -> List[PyColumnProfile]: ...
def analyze_csv_with_quality(path: str) -> PyQualityReport: ...
def analyze_json_file(path: str) -> List[PyColumnProfile]: ...
def analyze_json_with_quality(path: str) -> PyQualityReport: ...
def calculate_data_quality_metrics(path: str) -> PyDataQualityMetrics: ...

# Parquet analysis functions (available with parquet feature)
def analyze_parquet_file(path: str) -> List[PyColumnProfile]: ...
def analyze_parquet_with_quality_py(path: str) -> PyQualityReport: ...

# Batch processing functions
# Supports CSV, JSON, JSONL, and Parquet files (Parquet requires parquet feature)
def batch_analyze_glob(
    pattern: str,
    parallel: Optional[bool] = None,
    max_concurrent: Optional[int] = None,
    html_output: Optional[str] = None,
) -> PyBatchResult: ...

def batch_analyze_directory(
    directory: str,
    recursive: Optional[bool] = None,
    parallel: Optional[bool] = None,
    max_concurrent: Optional[int] = None,
    html_output: Optional[str] = None,
) -> PyBatchResult: ...

# Python logging integration
def configure_logging(level: Optional[str] = None, format: Optional[str] = None) -> None: ...
def get_logger(name: Optional[str] = None) -> Any: ...
def log_info(message: str, logger_name: Optional[str] = None) -> None: ...
def log_debug(message: str, logger_name: Optional[str] = None) -> None: ...
def log_warning(message: str, logger_name: Optional[str] = None) -> None: ...
def log_error(message: str, logger_name: Optional[str] = None) -> None: ...

# Enhanced analysis functions with logging
def analyze_csv_with_logging(file_path: str, log_level: Optional[str] = None) -> List[PyColumnProfile]: ...

# Pandas integration (conditional)
if _PANDAS_AVAILABLE:
    def analyze_csv_dataframe(file_path: str) -> pd.DataFrame: ...
else:
    def analyze_csv_dataframe(file_path: str) -> Any: ...

# Core profiling classes exported from Rust
class PyColumnProfile:
    """Column profiling information from Rust."""

    name: str
    data_type: str
    total_count: int
    null_count: int
    unique_count: Optional[int]
    null_percentage: float
    uniqueness_ratio: float

    def __new__(cls) -> Self: ...

class PyQualityReport:
    """Complete quality report for a dataset."""

    file_path: str
    total_rows: Optional[int]
    total_columns: int
    column_profiles: List[PyColumnProfile]
    rows_scanned: int
    sampling_ratio: float
    scan_time_ms: int
    data_quality_metrics: PyDataQualityMetrics

    def __new__(cls) -> Self: ...
    def quality_score(self) -> float: ...
    def to_json(self) -> str: ...

class PyDataQualityMetrics:
    """ISO 8000/25012 compliant data quality metrics."""

    # Overall Score
    overall_quality_score: float

    # Completeness
    missing_values_ratio: float
    complete_records_ratio: float
    null_columns: List[str]

    # Consistency
    data_type_consistency: float
    format_violations: int
    encoding_issues: int

    # Uniqueness
    duplicate_rows: int
    key_uniqueness: float
    high_cardinality_warning: bool

    # Accuracy
    outlier_ratio: float
    range_violations: int
    negative_values_in_positive: int

    # Timeliness (ISO 8000-8)
    future_dates_count: int
    stale_data_ratio: float
    temporal_violations: int

    def __new__(cls) -> Self: ...
    def completeness_summary(self) -> str: ...
    def consistency_summary(self) -> str: ...
    def uniqueness_summary(self) -> str: ...
    def accuracy_summary(self) -> str: ...
    def timeliness_summary(self) -> str: ...
    def summary_dict(self) -> Dict[str, str]: ...
    def _repr_html_(self) -> str: ...
    def __str__(self) -> str: ...

class PyBatchResult:
    """Result of batch processing operation."""

    processed_files: int
    failed_files: int
    total_duration_secs: float
    average_quality_score: float

    def __new__(cls) -> Self: ...

# Context Manager Classes
class PyBatchAnalyzer:
    """Context manager for batch analysis with automatic cleanup.

    Supports automatic format detection for CSV, JSON, JSONL, and Parquet files.
    Files are analyzed using the appropriate parser based on file extension.
    """

    def __new__(cls) -> Self: ...
    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool: ...
    def add_file(self, path: str) -> None:
        """Add a file to analysis queue (auto-detects CSV/JSON/Parquet format)."""
        ...
    def add_temp_file(self, path: str) -> None: ...
    def get_results(self) -> List[Any]: ...
    def analyze_batch(self, paths: List[str]) -> List[Any]:
        """Analyze multiple files in batch (auto-detects CSV/JSON/Parquet format)."""
        ...

class PyCsvProcessor:
    """Context manager for CSV file processing with automatic handling."""

    def __new__(cls, chunk_size: Optional[int] = None) -> Self: ...
    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool: ...
    def open_file(self, path: str) -> None: ...
    def process_chunks(self) -> List[Any]: ...
    def get_processing_info(self) -> Dict[str, Any]: ...

# Export all public classes and functions
__all__ = [
    # Core analysis functions
    "analyze_csv_file",
    "analyze_csv_with_quality",
    "analyze_json_file",
    "analyze_json_with_quality",
    "analyze_parquet_file",
    "analyze_parquet_with_quality_py",
    "calculate_data_quality_metrics",
    "batch_analyze_glob",
    "batch_analyze_directory",

    # Python logging integration
    "configure_logging",
    "get_logger",
    "log_info",
    "log_debug",
    "log_warning",
    "log_error",

    # Enhanced analysis with logging
    "analyze_csv_with_logging",

    # Pandas integration
    "analyze_csv_dataframe",

    # Core classes
    "PyColumnProfile",
    "PyQualityReport",
    "PyDataQualityMetrics",
    "PyBatchResult",

    # Context managers
    "PyBatchAnalyzer",
    "PyCsvProcessor",
]
