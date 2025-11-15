"""DataProf Python bindings"""

from .dataprof import *

__version__ = "0.3.0"

# Core exports for data profiling
__all__ = [
    # Core analysis functions
    "analyze_csv_file",
    "analyze_csv_with_quality",
    "analyze_json_file",
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

    # Core classes
    "PyColumnProfile",
    "PyQualityReport",
    "PyDataQualityMetrics",
    "PyBatchResult",

    # Context managers
    "PyBatchAnalyzer",
    "PyCsvProcessor",
]
