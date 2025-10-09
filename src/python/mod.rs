#![allow(clippy::useless_conversion)]

pub mod analysis;
pub mod batch;
pub mod dataframe;
pub mod logging;
pub mod processor;
pub mod types;

// Re-export all public types and functions
pub use analysis::{
    analyze_csv_file, analyze_csv_with_quality, analyze_json_file, analyze_json_with_quality,
    calculate_data_quality_metrics,
};

#[cfg(feature = "parquet")]
pub use analysis::{analyze_parquet_file, analyze_parquet_with_quality_py};
pub use batch::{batch_analyze_directory, batch_analyze_glob, PyBatchAnalyzer};
pub use dataframe::analyze_csv_dataframe;
pub use logging::{
    analyze_csv_with_logging, configure_logging, get_logger, log_debug, log_error, log_info,
    log_warning,
};
pub use processor::PyCsvProcessor;
pub use types::{PyBatchResult, PyColumnProfile, PyDataQualityMetrics, PyQualityReport};

use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::wrap_pyfunction;

/// Python module definition
#[pymodule]
pub fn dataprof(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Add version information from Cargo.toml
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    // Core data profiling classes
    m.add_class::<PyColumnProfile>()?;
    m.add_class::<PyQualityReport>()?;
    m.add_class::<PyDataQualityMetrics>()?;
    m.add_class::<PyBatchResult>()?;

    // Context manager classes
    m.add_class::<PyBatchAnalyzer>()?;
    m.add_class::<PyCsvProcessor>()?;

    // Single file analysis
    m.add_function(wrap_pyfunction!(analyze_csv_file, m)?)?;
    m.add_function(wrap_pyfunction!(analyze_csv_with_quality, m)?)?;
    m.add_function(wrap_pyfunction!(analyze_json_file, m)?)?;
    m.add_function(wrap_pyfunction!(analyze_json_with_quality, m)?)?;
    m.add_function(wrap_pyfunction!(calculate_data_quality_metrics, m)?)?;

    // Parquet analysis (only available with parquet feature)
    #[cfg(feature = "parquet")]
    {
        m.add_function(wrap_pyfunction!(analyze_parquet_file, m)?)?;
        m.add_function(wrap_pyfunction!(analyze_parquet_with_quality_py, m)?)?;
    }

    // Pandas integration (optional)
    m.add_function(wrap_pyfunction!(analyze_csv_dataframe, m)?)?;

    // Batch processing
    m.add_function(wrap_pyfunction!(batch_analyze_glob, m)?)?;
    m.add_function(wrap_pyfunction!(batch_analyze_directory, m)?)?;

    // Python logging integration
    m.add_function(wrap_pyfunction!(configure_logging, m)?)?;
    m.add_function(wrap_pyfunction!(get_logger, m)?)?;
    m.add_function(wrap_pyfunction!(log_info, m)?)?;
    m.add_function(wrap_pyfunction!(log_debug, m)?)?;
    m.add_function(wrap_pyfunction!(log_warning, m)?)?;
    m.add_function(wrap_pyfunction!(log_error, m)?)?;

    // Enhanced analysis functions with logging
    m.add_function(wrap_pyfunction!(analyze_csv_with_logging, m)?)?;

    // Note: Async functions temporarily disabled due to compatibility issues

    Ok(())
}
