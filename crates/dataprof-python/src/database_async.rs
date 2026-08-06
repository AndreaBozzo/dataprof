//! Async Python bindings for database operations
//!
//! This module provides async Python functions for database profiling,
//! allowing non-blocking database queries from Python using asyncio.

use pyo3::prelude::*;

use crate::config::PyProfilerConfig;
use crate::errors::analysis_error_to_py;
use crate::types::PyProfileReport;
#[cfg(feature = "database")]
use dataprof::{
    AnalysisOptions, DataProfilerError, DatabaseConfig, MetricPack, analyze_database_with_options,
    create_connector,
};

/// Async Python wrapper for database analysis
///
/// This function allows Python code to analyze database queries asynchronously
/// using Python's asyncio framework.
///
/// # Arguments
/// * `connection_string` - Database connection string (postgres://, mysql://, sqlite://)
/// * `query` - SQL query to analyze
/// * `batch_size` - Optional batch size for streaming (default: 10000)
/// * `calculate_quality` - Whether to calculate quality metrics (default: false)
/// * `config` - Optional profiler config carrying `metrics`, `quality_dimensions`,
///   and `locale`, honoured the same way every file path honours them
///
/// # Returns
/// A dictionary containing column profiles and optional quality report
///
/// # Example (Python)
/// ```python
/// import asyncio
/// import dataprof
/// from dataprof import ProfilerConfig
///
/// async def analyze_db():
///     result = await dataprof.analyze_database_async(
///         "postgresql://user:pass@localhost/db",
///         "SELECT * FROM users LIMIT 1000",
///         batch_size=1000,
///         calculate_quality=True,
///         config=ProfilerConfig(locale="IT"),
///     )
///     print(result)
///
/// asyncio.run(analyze_db())
/// ```
#[pyfunction]
#[pyo3(signature = (connection_string, query, batch_size=10000, calculate_quality=false, config=None))]
pub fn analyze_database_async<'py>(
    py: Python<'py>,
    connection_string: String,
    query: String,
    batch_size: usize,
    calculate_quality: bool,
    config: Option<&PyProfilerConfig>,
) -> PyResult<Bound<'py, PyAny>> {
    if batch_size == 0 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "batch_size must be greater than zero",
        ));
    }
    // Resolve the selection before the future: `config` is borrowed from Python
    // and cannot cross the await boundary.
    #[cfg(all(feature = "database", feature = "python-async"))]
    let options = {
        let selected = config
            .map(PyProfilerConfig::analysis_options)
            .unwrap_or_default();
        if calculate_quality {
            selected
        } else {
            // `calculate_quality=False` is the older, coarser way to say "no
            // quality pack"; express it as a pack so one value carries the
            // whole selection from here on.
            let packs = selected
                .effective_metric_packs()
                .unwrap_or_else(MetricPack::all)
                .into_iter()
                .filter(|pack| *pack != MetricPack::Quality)
                .collect();
            selected.with_metric_packs(Some(packs))
        }
    };
    #[cfg(not(all(feature = "database", feature = "python-async")))]
    let _ = (config, calculate_quality);

    // Import pyo3_async_runtimes only when python-async feature is enabled
    #[cfg(feature = "python-async")]
    {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            analyze_database_internal(connection_string, query, batch_size, options)
                .await
                .map_err(|e| analysis_error_to_py(&e))
        })
    }

    #[cfg(not(feature = "python-async"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Async support not enabled. Please compile with --features python-async",
        ))
    }
}

/// Internal async function that does the actual database analysis
#[cfg(all(feature = "database", feature = "python-async"))]
async fn analyze_database_internal(
    connection_string: String,
    query: String,
    batch_size: usize,
    options: AnalysisOptions,
) -> Result<PyProfileReport, DataProfilerError> {
    // Create database configuration
    let config = DatabaseConfig {
        connection_string,
        batch_size,
        ..Default::default()
    };

    // Analyze the database query
    let report = analyze_database_with_options(config, &query, &options).await?;

    Ok(PyProfileReport::new(report))
}

/// Test async database connection
///
/// # Arguments
/// * `connection_string` - Database connection string
///
/// # Returns
/// True if connection successful, False otherwise
///
/// # Example (Python)
/// ```python
/// import asyncio
/// import dataprof
///
/// async def test():
///     connected = await dataprof.test_connection_async(
///         "postgresql://localhost/testdb"
///     )
///     print(f"Connected: {connected}")
///
/// asyncio.run(test())
/// ```
#[pyfunction]
pub fn test_connection_async<'py>(
    py: Python<'py>,
    connection_string: String,
) -> PyResult<Bound<'py, PyAny>> {
    #[cfg(feature = "python-async")]
    {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            test_connection_internal(connection_string)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }

    #[cfg(not(feature = "python-async"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Async support not enabled. Please compile with --features python-async",
        ))
    }
}

/// Internal test connection function
#[cfg(all(feature = "database", feature = "python-async"))]
async fn test_connection_internal(connection_string: String) -> Result<bool, DataProfilerError> {
    let config = DatabaseConfig {
        connection_string,
        ..Default::default()
    };

    let mut connector = create_connector(config)?;
    connector.connect().await?;
    let result = connector.test_connection().await;
    connector.disconnect().await?;
    result
}

/// Get table schema asynchronously
///
/// # Arguments
/// * `connection_string` - Database connection string
/// * `table_name` - Name of the table
///
/// # Returns
/// List of column names
///
/// # Example (Python)
/// ```python
/// import asyncio
/// import dataprof
///
/// async def get_schema():
///     columns = await dataprof.get_table_schema_async(
///         "postgresql://localhost/testdb",
///         "users"
///     )
///     print(f"Columns: {columns}")
///
/// asyncio.run(get_schema())
/// ```
#[pyfunction]
pub fn get_table_schema_async<'py>(
    py: Python<'py>,
    connection_string: String,
    table_name: String,
) -> PyResult<Bound<'py, PyAny>> {
    #[cfg(feature = "python-async")]
    {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            get_table_schema_internal(connection_string, table_name)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }

    #[cfg(not(feature = "python-async"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Async support not enabled. Please compile with --features python-async",
        ))
    }
}

/// Internal get schema function
#[cfg(all(feature = "database", feature = "python-async"))]
async fn get_table_schema_internal(
    connection_string: String,
    table_name: String,
) -> Result<Vec<String>, DataProfilerError> {
    let config = DatabaseConfig {
        connection_string,
        ..Default::default()
    };

    let mut connector = create_connector(config)?;
    connector.connect().await?;
    let schema = connector.get_table_schema(&table_name).await?;
    connector.disconnect().await?;

    Ok(schema)
}

/// Count rows in a table asynchronously
///
/// # Arguments
/// * `connection_string` - Database connection string
/// * `table_name` - Name of the table
///
/// # Returns
/// Number of rows in the table
///
/// # Example (Python)
/// ```python
/// import asyncio
/// import dataprof
///
/// async def count_rows():
///     count = await dataprof.count_table_rows_async(
///         "postgresql://localhost/testdb",
///         "users"
///     )
///     print(f"Row count: {count}")
///
/// asyncio.run(count_rows())
/// ```
#[pyfunction]
pub fn count_table_rows_async<'py>(
    py: Python<'py>,
    connection_string: String,
    table_name: String,
) -> PyResult<Bound<'py, PyAny>> {
    #[cfg(feature = "python-async")]
    {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            count_table_rows_internal(connection_string, table_name)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }

    #[cfg(not(feature = "python-async"))]
    {
        Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Async support not enabled. Please compile with --features python-async",
        ))
    }
}

/// Internal count rows function
#[cfg(all(feature = "database", feature = "python-async"))]
async fn count_table_rows_internal(
    connection_string: String,
    table_name: String,
) -> Result<u64, DataProfilerError> {
    let config = DatabaseConfig {
        connection_string,
        ..Default::default()
    };

    let mut connector = create_connector(config)?;
    connector.connect().await?;
    let count = connector.count_table_rows(&table_name).await?;
    connector.disconnect().await?;

    Ok(count)
}
