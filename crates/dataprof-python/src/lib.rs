#![allow(clippy::useless_conversion)]

pub mod analysis;
pub mod arrow_export;
pub mod columns;
pub mod config;
pub mod partial;
pub mod progress;
pub mod sampling;
pub mod stop_condition;
pub mod types;

#[cfg(all(feature = "python-async", feature = "database"))]
pub mod database_async;

#[cfg(all(feature = "python-async", feature = "async-streaming"))]
pub mod async_streaming;

// Re-exports for module registration
pub use analysis::{analyze_file, list_patterns};
pub use arrow_export::{
    PyRecordBatch, analyze_csv_to_arrow, analyze_parquet_to_arrow, profile_arrow, profile_dataframe,
};
pub use columns::profile_columns;
pub use config::PyProfilerConfig;
pub use partial::{
    PyRowCountEstimate, PySchemaResult, PyStructureColumnSummary, PyStructureReport,
    analyze_structure, infer_schema, quick_row_count,
};
pub use progress::PyProgressEvent;
pub use sampling::PySamplingStrategy;
pub use stop_condition::PyStopCondition;
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
    m.add_class::<PyStructureReport>()?;
    m.add_class::<PyStructureColumnSummary>()?;

    // Sampling, stop conditions, and progress
    m.add_class::<PySamplingStrategy>()?;
    m.add_class::<PyStopCondition>()?;
    m.add_class::<PyProgressEvent>()?;

    // Core analysis
    m.add_function(wrap_pyfunction!(analyze_file, m)?)?;
    m.add_function(wrap_pyfunction!(list_patterns, m)?)?;

    // DataFrame/Arrow profiling
    m.add_function(wrap_pyfunction!(profile_dataframe, m)?)?;
    m.add_function(wrap_pyfunction!(profile_arrow, m)?)?;

    // Dependency-free columnar profiling (dict, list-of-dicts, decoded bytes)
    m.add_function(wrap_pyfunction!(profile_columns, m)?)?;

    // Arrow export
    m.add_function(wrap_pyfunction!(analyze_csv_to_arrow, m)?)?;
    m.add_function(wrap_pyfunction!(analyze_parquet_to_arrow, m)?)?;

    // Partial analysis
    m.add_function(wrap_pyfunction!(infer_schema, m)?)?;
    m.add_function(wrap_pyfunction!(quick_row_count, m)?)?;
    m.add_function(wrap_pyfunction!(analyze_structure, m)?)?;

    // Async database functions (feature-gated)
    #[cfg(all(feature = "python-async", feature = "database"))]
    {
        m.add_function(wrap_pyfunction!(database_async::analyze_database_async, m)?)?;
        m.add_function(wrap_pyfunction!(database_async::test_connection_async, m)?)?;
        m.add_function(wrap_pyfunction!(database_async::get_table_schema_async, m)?)?;
        m.add_function(wrap_pyfunction!(database_async::count_table_rows_async, m)?)?;
    }

    // Async streaming functions (feature-gated)
    #[cfg(all(feature = "python-async", feature = "async-streaming"))]
    {
        m.add_function(wrap_pyfunction!(async_streaming::profile_bytes_async, m)?)?;
        m.add_function(wrap_pyfunction!(async_streaming::profile_file_async, m)?)?;
        m.add_function(wrap_pyfunction!(async_streaming::profile_url_async, m)?)?;
        m.add_function(wrap_pyfunction!(
            async_streaming::infer_schema_stream_async,
            m
        )?)?;
        m.add_function(wrap_pyfunction!(
            async_streaming::quick_row_count_stream_async,
            m
        )?)?;
    }

    Ok(())
}
