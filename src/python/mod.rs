#![allow(clippy::useless_conversion)]

pub mod analysis;
pub mod arrow_export;
pub mod batch;
pub mod dataframe;
pub mod logging;
pub mod processor;
pub mod types;

#[cfg(all(feature = "python-async", feature = "database"))]
pub mod database_async;

// Re-export all public types and functions
pub use analysis::{
    analyze_csv_file, analyze_csv_with_quality, analyze_json_file, analyze_json_with_quality,
    calculate_data_quality_metrics,
};

#[cfg(feature = "parquet")]
pub use analysis::{analyze_parquet_file, analyze_parquet_with_quality_py};
pub use batch::{PyBatchAnalyzer, batch_analyze_directory, batch_analyze_glob};
pub use dataframe::analyze_csv_dataframe;
pub use logging::{
    analyze_csv_with_logging, configure_logging, get_logger, log_debug, log_error, log_info,
    log_warning,
};
pub use processor::PyCsvProcessor;
pub use types::{PyBatchResult, PyColumnProfile, PyDataQualityMetrics, PyQualityReport};

// Arrow/PyCapsule exports
#[cfg(feature = "parquet")]
pub use arrow_export::analyze_parquet_to_arrow;
pub use arrow_export::{PyRecordBatch, analyze_csv_to_arrow, profile_arrow, profile_dataframe};

use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::wrap_pyfunction;

/// Python module definition
#[pymodule]
#[pyo3(name = "_dataprof")]
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

    // Arrow/PyCapsule interface for zero-copy data exchange
    m.add_class::<PyRecordBatch>()?;
    m.add_function(wrap_pyfunction!(analyze_csv_to_arrow, m)?)?;
    m.add_function(wrap_pyfunction!(profile_dataframe, m)?)?;
    m.add_function(wrap_pyfunction!(profile_arrow, m)?)?;

    #[cfg(feature = "parquet")]
    m.add_function(wrap_pyfunction!(analyze_parquet_to_arrow, m)?)?;

    // Async database functions (available with python-async and database features)
    #[cfg(all(feature = "python-async", feature = "database"))]
    {
        m.add_function(wrap_pyfunction!(database_async::analyze_database_async, m)?)?;
        m.add_function(wrap_pyfunction!(database_async::test_connection_async, m)?)?;
        m.add_function(wrap_pyfunction!(database_async::get_table_schema_async, m)?)?;
        m.add_function(wrap_pyfunction!(database_async::count_table_rows_async, m)?)?;
    }

    Ok(())
}
