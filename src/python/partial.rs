use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::path::Path;

use crate::api::partial;
use crate::types::{CountMethod, DataType, RowCountEstimate, SchemaResult};

/// Result of fast schema inference.
#[pyclass(name = "SchemaResult")]
pub struct PySchemaResult {
    inner: SchemaResult,
}

#[pymethods]
impl PySchemaResult {
    /// List of columns as dicts with "name" and "data_type" keys
    #[getter]
    fn columns(&self) -> Vec<std::collections::HashMap<String, String>> {
        self.inner
            .columns
            .iter()
            .map(|c| {
                let mut m = std::collections::HashMap::new();
                m.insert("name".to_string(), c.name.clone());
                m.insert(
                    "data_type".to_string(),
                    match c.data_type {
                        DataType::Integer => "integer",
                        DataType::Float => "float",
                        DataType::String => "string",
                        DataType::Date => "date",
                    }
                    .to_string(),
                );
                m
            })
            .collect()
    }

    /// Number of rows sampled to infer the schema (0 for Parquet metadata)
    #[getter]
    fn rows_sampled(&self) -> usize {
        self.inner.rows_sampled
    }

    /// Time taken for inference in milliseconds
    #[getter]
    fn inference_time_ms(&self) -> u128 {
        self.inner.inference_time_ms
    }

    /// Whether the schema is considered stable
    #[getter]
    fn schema_stable(&self) -> bool {
        self.inner.schema_stable
    }

    /// Number of columns detected
    #[getter]
    fn num_columns(&self) -> usize {
        self.inner.columns.len()
    }

    /// Column names as a list
    #[getter]
    fn column_names(&self) -> Vec<String> {
        self.inner.columns.iter().map(|c| c.name.clone()).collect()
    }

    fn __repr__(&self) -> String {
        format!(
            "SchemaResult(columns={}, rows_sampled={}, stable={}, time={}ms)",
            self.inner.columns.len(),
            self.inner.rows_sampled,
            self.inner.schema_stable,
            self.inner.inference_time_ms,
        )
    }
}

/// Result of a quick row count operation.
#[pyclass(name = "RowCountEstimate")]
pub struct PyRowCountEstimate {
    inner: RowCountEstimate,
}

#[pymethods]
impl PyRowCountEstimate {
    /// The estimated or exact row count
    #[getter]
    fn count(&self) -> u64 {
        self.inner.count
    }

    /// Whether the count is exact or an estimate
    #[getter]
    fn exact(&self) -> bool {
        self.inner.exact
    }

    /// How the count was obtained
    #[getter]
    fn method(&self) -> &str {
        match self.inner.method {
            CountMethod::ParquetMetadata => "parquet_metadata",
            CountMethod::FullScan => "full_scan",
            CountMethod::Sampling => "sampling",
            CountMethod::StreamFullScan => "stream_full_scan",
        }
    }

    /// Time taken in milliseconds
    #[getter]
    fn count_time_ms(&self) -> u128 {
        self.inner.count_time_ms
    }

    fn __repr__(&self) -> String {
        format!(
            "RowCountEstimate(count={}, exact={}, method='{}', time={}ms)",
            self.inner.count,
            self.inner.exact,
            self.method(),
            self.inner.count_time_ms,
        )
    }
}

/// Infer the schema (column names + data types) of a file.
///
/// Much faster than full profiling — reads only a small sample
/// (or just metadata for Parquet).
#[pyfunction]
pub fn infer_schema(path: &str) -> PyResult<PySchemaResult> {
    let result = partial::infer_schema(Path::new(path))
        .map_err(|e| PyRuntimeError::new_err(format!("Schema inference failed: {}", e)))?;
    Ok(PySchemaResult { inner: result })
}

/// Quick row count (exact or estimated) for a file.
///
/// Returns an exact count for small files and Parquet; an estimate
/// for large CSV/JSON files via sampling.
#[pyfunction]
pub fn quick_row_count(path: &str) -> PyResult<PyRowCountEstimate> {
    let result = partial::quick_row_count(Path::new(path))
        .map_err(|e| PyRuntimeError::new_err(format!("Row count failed: {}", e)))?;
    Ok(PyRowCountEstimate { inner: result })
}
