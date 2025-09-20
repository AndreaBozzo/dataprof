"""
Type stubs for dataprof Python bindings.

This module provides data profiling and ML readiness assessment functionality
implemented in Rust with Python bindings via PyO3.
"""

from typing import Any, Dict, List, Optional, Union
from typing_extensions import Self

try:
    import pandas as pd
    _PANDAS_AVAILABLE = True
except ImportError:
    _PANDAS_AVAILABLE = False

__version__: str

# Core analysis functions
def analyze_csv(file_path: str) -> List[Dict[str, Any]]: ...
def analyze_json(file_path: str) -> List[Dict[str, Any]]: ...
def quality_score(file_path: str) -> Dict[str, Any]: ...

# Python logging integration
def configure_logging(level: Optional[str] = None, format: Optional[str] = None) -> None: ...
def get_logger(name: Optional[str] = None) -> Any: ...
def log_info(message: str, logger_name: Optional[str] = None) -> None: ...
def log_debug(message: str, logger_name: Optional[str] = None) -> None: ...
def log_warning(message: str, logger_name: Optional[str] = None) -> None: ...
def log_error(message: str, logger_name: Optional[str] = None) -> None: ...

# Enhanced analysis functions with logging
def analyze_csv_with_logging(file_path: str, log_level: Optional[str] = None) -> List[Dict[str, Any]]: ...
def ml_readiness_score_with_logging(file_path: str, log_level: Optional[str] = None) -> PyMlReadinessScore: ...

# ML readiness functions
def ml_readiness_score(file_path: str) -> PyMlReadinessScore: ...
def analyze_csv_for_ml(file_path: str) -> List[PyFeatureAnalysis]: ...

# Pandas integration functions (conditional)
if _PANDAS_AVAILABLE:
    def analyze_csv_dataframe(file_path: str) -> pd.DataFrame: ...
    def feature_analysis_dataframe(file_path: str) -> pd.DataFrame: ...
else:
    def analyze_csv_dataframe(file_path: str) -> Any: ...
    def feature_analysis_dataframe(file_path: str) -> Any: ...

# Note: Async functions temporarily disabled due to compatibility issues

# ML Readiness Classes
class PyMlReadinessScore:
    """Python wrapper for ML readiness assessment results."""

    overall_score: float
    readiness_level: str
    completeness_score: float
    consistency_score: float
    type_suitability_score: float
    feature_quality_score: float
    blocking_issues: List[PyMlBlockingIssue]
    recommendations: List[PyMlRecommendation]

    def __new__(cls) -> Self: ...
    def summary(self) -> str:
        """Get a human-readable summary of the ML readiness assessment."""
        ...
    def to_dict(self) -> Dict[str, Any]:
        """Convert the ML readiness score to a dictionary."""
        ...
    def _repr_html_(self) -> str:
        """Rich HTML representation for Jupyter notebooks."""
        ...

class PyFeatureAnalysis:
    """Python wrapper for individual feature analysis results."""

    column_name: str
    feature_type: str
    ml_suitability: float
    importance_potential: str
    preprocessing_suggestions: List[PyPreprocessingSuggestion]

    def __new__(cls) -> Self: ...
    def to_dict(self) -> Dict[str, Any]:
        """Convert the feature analysis to a dictionary."""
        ...

class PyMlRecommendation:
    """Python wrapper for ML recommendation."""

    category: str
    priority: str
    message: str
    affected_columns: List[str]

    def __new__(cls) -> Self: ...
    def to_dict(self) -> Dict[str, Any]:
        """Convert the recommendation to a dictionary."""
        ...

class PyMlBlockingIssue:
    """Python wrapper for ML blocking issue."""

    issue_type: str
    severity: str
    message: str
    affected_columns: List[str]

    def __new__(cls) -> Self: ...
    def to_dict(self) -> Dict[str, Any]:
        """Convert the blocking issue to a dictionary."""
        ...

class PyPreprocessingSuggestion:
    """Python wrapper for preprocessing suggestion."""

    technique: str
    reason: str
    priority: str

    def __new__(cls) -> Self: ...
    def to_dict(self) -> Dict[str, Any]:
        """Convert the preprocessing suggestion to a dictionary."""
        ...

# Core profiling classes (existing)
class ColumnProfile:
    """Column profiling information."""

    name: str
    data_type: str
    null_count: int
    null_percentage: float
    unique_count: int
    unique_percentage: float

    def __new__(cls) -> Self: ...

class QualityScore:
    """Data quality scoring information."""

    overall_score: float
    completeness_score: float
    uniqueness_score: float
    validity_score: float

    def __new__(cls) -> Self: ...

# Context Manager Classes
class PyBatchAnalyzer:
    """Context manager for batch analysis with automatic cleanup."""

    def __new__(cls) -> Self: ...
    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool: ...
    def add_file(self, path: str) -> None: ...
    def add_temp_file(self, path: str) -> None: ...
    def get_results(self) -> List[Any]: ...
    def analyze_batch(self, paths: List[str]) -> List[Any]: ...

class PyMlAnalyzer:
    """Context manager for ML analysis with resource tracking."""

    def __new__(cls) -> Self: ...
    def __enter__(self) -> Self: ...
    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool: ...
    def analyze_ml(self, path: str) -> PyMlReadinessScore: ...
    def add_temp_file(self, path: str) -> None: ...
    def get_stats(self) -> Dict[str, float]: ...
    def get_ml_results(self) -> List[PyMlReadinessScore]: ...
    def get_summary(self) -> Optional[Dict[str, Any]]: ...

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
    # Core functions
    "analyze_csv",
    "analyze_json",
    "quality_score",

    # Python logging integration
    "configure_logging",
    "get_logger",
    "log_info",
    "log_debug",
    "log_warning",
    "log_error",

    # Enhanced analysis with logging
    "analyze_csv_with_logging",
    "ml_readiness_score_with_logging",

    # ML readiness functions
    "ml_readiness_score",
    "analyze_csv_for_ml",

    # Pandas integration
    "analyze_csv_dataframe",
    "feature_analysis_dataframe",

    # Note: Async functions temporarily disabled

    # ML classes
    "PyMlReadinessScore",
    "PyFeatureAnalysis",
    "PyMlRecommendation",
    "PyMlBlockingIssue",
    "PyPreprocessingSuggestion",

    # Core classes
    "ColumnProfile",
    "QualityScore",

    # Context managers
    "PyBatchAnalyzer",
    "PyMlAnalyzer",
    "PyCsvProcessor",
]
