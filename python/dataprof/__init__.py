"""DataProf Python bindings"""

from .dataprof import *

__version__ = "0.3.0"

# ML readiness exports for easy access
__all__ = [
    # Core analysis functions
    "analyze_csv_file",
    "analyze_csv_with_quality",
    "analyze_json_file",
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
    "ml_readiness_score_with_logging",

    # ML readiness functions
    "ml_readiness_score",
    "analyze_csv_for_ml",

    # Note: Async functions temporarily disabled

    # Core classes
    "PyColumnProfile",
    "PyQualityReport",
    "PyQualityIssue",
    "PyBatchResult",

    # ML readiness classes
    "PyMlReadinessScore",
    "PyMlRecommendation",
    "PyMlBlockingIssue",
    "PyFeatureAnalysis",
    "PyPreprocessingSuggestion",

    # Context managers
    "PyBatchAnalyzer",
    "PyMlAnalyzer",
    "PyCsvProcessor",
]
