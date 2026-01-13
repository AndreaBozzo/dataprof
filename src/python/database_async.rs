//! Async Python bindings for database operations
//!
//! This module provides async Python functions for database profiling,
//! allowing non-blocking database queries from Python using asyncio.

use pyo3::prelude::*;
use pyo3::types::PyDict;

#[cfg(feature = "database")]
use crate::database::{DatabaseConfig, create_connector, analyze_database};
use crate::python::types::{PyColumnProfile, PyDataQualityMetrics};

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
///
/// # Returns
/// A dictionary containing column profiles and optional quality report
///
/// # Example (Python)
/// ```python
/// import asyncio
/// import dataprof
///
/// async def analyze_db():
///     result = await dataprof.analyze_database_async(
///         "postgresql://user:pass@localhost/db",
///         "SELECT * FROM users LIMIT 1000",
///         batch_size=1000,
///         calculate_quality=True
///     )
///     print(result)
///
/// asyncio.run(analyze_db())
/// ```
#[pyfunction]
#[pyo3(signature = (connection_string, query, batch_size=10000, calculate_quality=false))]
pub fn analyze_database_async<'py>(
    py: Python<'py>,
    connection_string: String,
    query: String,
    batch_size: usize,
    calculate_quality: bool,
) -> PyResult<Bound<'py, PyAny>> {
    // Import pyo3_async_runtimes only when python-async feature is enabled
    #[cfg(feature = "python-async")]
    {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            analyze_database_internal(connection_string, query, batch_size, calculate_quality)
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

/// Internal async function that does the actual database analysis
#[cfg(all(feature = "database", feature = "python-async"))]
async fn analyze_database_internal(
    connection_string: String,
    query: String,
    batch_size: usize,
    _calculate_quality: bool,
) -> Result<pyo3::Py<pyo3::PyAny>, anyhow::Error> {
    use pyo3::Python;

    // Create database configuration
    let config = DatabaseConfig {
        connection_string,
        batch_size,
        ..Default::default()
    };

    // Analyze the database query
    let quality_report = analyze_database(config, &query).await?;

    // Convert to Python objects
    Python::attach(|py| {
        let result = PyDict::new(py);

        // Add column profiles as a list
        let py_profiles: Vec<PyColumnProfile> = quality_report
            .column_profiles
            .iter()
            .map(PyColumnProfile::from)
            .collect();
        result.set_item("columns", py_profiles)?;

        // Add quality metrics
        result.set_item(
            "quality",
            PyDataQualityMetrics::from(&quality_report.data_quality_metrics),
        )?;

        // Add metadata
        result.set_item("row_count", quality_report.file_info.total_rows)?;

        // Convert scan_info to a simple string representation
        let scan_info = format!("{:?}", quality_report.scan_info);
        result.set_item("scan_info", scan_info)?;

        Ok(result.into())
    })
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
async fn test_connection_internal(connection_string: String) -> Result<bool, anyhow::Error> {
    let config = DatabaseConfig {
        connection_string,
        ..Default::default()
    };

    let mut connector = create_connector(config)?;
    connector.test_connection().await
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
) -> Result<Vec<String>, anyhow::Error> {
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
) -> Result<u64, anyhow::Error> {
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
