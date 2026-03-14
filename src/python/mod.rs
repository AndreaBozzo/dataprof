#![allow(clippy::useless_conversion)]

pub mod analysis;
pub mod arrow_export;
pub mod config;
pub mod partial;
pub mod types;

#[cfg(all(feature = "python-async", feature = "database"))]
pub mod database_async;

// Re-exports for module registration
pub use analysis::analyze_file;
pub use arrow_export::{
    PyRecordBatch, analyze_csv_to_arrow, analyze_parquet_to_arrow, profile_arrow, profile_dataframe,
};
pub use config::PyProfilerConfig;
pub use partial::{PyRowCountEstimate, PySchemaResult, infer_schema, quick_row_count};
pub use types::{PyColumnProfile, PyDataQualityMetrics, PyProfileReport};

use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::wrap_pyfunction;

/// Python module definition
#[pymodule]
#[pyo3(name = "_dataprof")]
pub fn dataprof(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Version
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    // Configuration
    m.add_class::<PyProfilerConfig>()?;

    // Result types
    m.add_class::<PyProfileReport>()?;
    m.add_class::<PyColumnProfile>()?;
    m.add_class::<PyDataQualityMetrics>()?;

    // Arrow interop
    m.add_class::<PyRecordBatch>()?;

    // Partial analysis types
    m.add_class::<PySchemaResult>()?;
    m.add_class::<PyRowCountEstimate>()?;

    // Core analysis
    m.add_function(wrap_pyfunction!(analyze_file, m)?)?;

    // DataFrame/Arrow profiling
    m.add_function(wrap_pyfunction!(profile_dataframe, m)?)?;
    m.add_function(wrap_pyfunction!(profile_arrow, m)?)?;

    // Arrow export
    m.add_function(wrap_pyfunction!(analyze_csv_to_arrow, m)?)?;
    m.add_function(wrap_pyfunction!(analyze_parquet_to_arrow, m)?)?;

    // Partial analysis
    m.add_function(wrap_pyfunction!(infer_schema, m)?)?;
    m.add_function(wrap_pyfunction!(quick_row_count, m)?)?;

    // Async database functions (feature-gated)
    #[cfg(all(feature = "python-async", feature = "database"))]
    {
        m.add_function(wrap_pyfunction!(database_async::analyze_database_async, m)?)?;
        m.add_function(wrap_pyfunction!(database_async::test_connection_async, m)?)?;
        m.add_function(wrap_pyfunction!(database_async::get_table_schema_async, m)?)?;
        m.add_function(wrap_pyfunction!(database_async::count_table_rows_async, m)?)?;
    }

    Ok(())
}
