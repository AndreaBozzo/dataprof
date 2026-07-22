use pyo3::prelude::*;
use std::path::Path;

use dataprof::{
    CountMethod, DataType, RowCountEstimate, SchemaResult, StructureColumnSummary, StructureReport,
};

use super::errors::analysis_error_to_py;

fn data_type_name(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Integer => "integer",
        DataType::Float => "float",
        DataType::String => "string",
        DataType::Identifier => "identifier",
        DataType::Date => "date",
        DataType::Boolean => "boolean",
    }
}

/// Result of fast schema inference.
#[pyclass(name = "SchemaResult")]
pub struct PySchemaResult {
    inner: SchemaResult,
}

impl PySchemaResult {
    pub fn from_inner(inner: SchemaResult) -> Self {
        Self { inner }
    }
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
                    data_type_name(&c.data_type).to_string(),
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

/// Structural summary for one column.
#[pyclass(name = "StructureColumnSummary", from_py_object)]
#[derive(Clone)]
pub struct PyStructureColumnSummary {
    inner: StructureColumnSummary,
}

impl PyStructureColumnSummary {
    pub fn from_inner(inner: StructureColumnSummary) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyStructureColumnSummary {
    #[getter]
    fn name(&self) -> &str {
        &self.inner.name
    }

    #[getter]
    fn data_type(&self) -> &str {
        data_type_name(&self.inner.data_type)
    }

    #[getter]
    fn total_count(&self) -> Option<usize> {
        self.inner.total_count
    }

    #[getter]
    fn null_count(&self) -> Option<usize> {
        self.inner.null_count
    }

    #[getter]
    fn null_ratio(&self) -> Option<f64> {
        self.inner.null_ratio
    }

    #[getter]
    fn unique_count(&self) -> Option<usize> {
        self.inner.unique_count
    }

    #[getter]
    fn uniqueness_ratio(&self) -> Option<f64> {
        self.inner.uniqueness_ratio
    }

    #[getter]
    fn distinct_count_approximate(&self) -> Option<bool> {
        self.inner.distinct_count_approximate
    }

    #[getter]
    fn provenance(&self) -> &str {
        &self.inner.provenance
    }

    fn __repr__(&self) -> String {
        format!(
            "StructureColumnSummary(name='{}', type='{}', provenance='{}')",
            self.inner.name,
            data_type_name(&self.inner.data_type),
            self.inner.provenance,
        )
    }
}

/// Lightweight structural report for a file.
#[pyclass(name = "StructureReport")]
pub struct PyStructureReport {
    inner: StructureReport,
}

impl PyStructureReport {
    pub fn from_inner(inner: StructureReport) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyStructureReport {
    #[getter]
    fn source(&self) -> &str {
        &self.inner.source
    }

    #[getter]
    fn format(&self) -> String {
        self.inner.format.to_string()
    }

    #[getter]
    fn row_count(&self) -> PyRowCountEstimate {
        PyRowCountEstimate::from_inner(self.inner.row_count.clone())
    }

    #[getter]
    fn rows_sampled(&self) -> usize {
        self.inner.rows_sampled
    }

    #[getter]
    fn source_exhausted(&self) -> bool {
        self.inner.source_exhausted
    }

    #[getter]
    fn truncated(&self) -> bool {
        self.inner.truncated
    }

    #[getter]
    fn truncation_reason(&self) -> Option<String> {
        self.inner.truncation_reason.clone()
    }

    #[getter]
    fn delimiter(&self) -> Option<String> {
        self.inner.delimiter.clone()
    }

    #[getter]
    fn columns(&self) -> Vec<PyStructureColumnSummary> {
        self.inner
            .columns
            .iter()
            .cloned()
            .map(PyStructureColumnSummary::from_inner)
            .collect()
    }

    #[getter]
    fn warnings(&self) -> Vec<String> {
        self.inner.warnings.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "StructureReport(source='{}', format='{}', columns={}, rows_sampled={}, truncated={})",
            self.inner.source,
            self.inner.format,
            self.inner.columns.len(),
            self.inner.rows_sampled,
            self.inner.truncated,
        )
    }
}

/// Result of a quick row count operation.
#[pyclass(name = "RowCountEstimate")]
pub struct PyRowCountEstimate {
    inner: RowCountEstimate,
}

impl PyRowCountEstimate {
    pub fn from_inner(inner: RowCountEstimate) -> Self {
        Self { inner }
    }
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

    /// Approximate 1-sigma relative standard error of an estimated count
    /// (fraction of `count`, e.g. 0.02 ≈ ±2%). `None` for exact counts.
    #[getter]
    fn relative_error(&self) -> Option<f64> {
        self.inner.relative_error
    }

    fn __repr__(&self) -> String {
        match self.inner.relative_error {
            Some(err) => format!(
                "RowCountEstimate(count={}, exact={}, method='{}', relative_error={:.4}, time={}ms)",
                self.inner.count,
                self.inner.exact,
                self.method(),
                err,
                self.inner.count_time_ms,
            ),
            None => format!(
                "RowCountEstimate(count={}, exact={}, method='{}', time={}ms)",
                self.inner.count,
                self.inner.exact,
                self.method(),
                self.inner.count_time_ms,
            ),
        }
    }
}

/// Infer the schema (column names + data types) of a file.
///
/// Much faster than full profiling — reads only a small sample
/// (or just metadata for Parquet).
#[pyfunction]
pub fn infer_schema(path: &str) -> PyResult<PySchemaResult> {
    let result = dataprof::infer_schema(Path::new(path)).map_err(|e| analysis_error_to_py(&e))?;
    Ok(PySchemaResult { inner: result })
}

/// Quick row count (exact or estimated) for a file.
///
/// Returns an exact count for small files and Parquet; an estimate
/// for large CSV/JSON files via sampling.
#[pyfunction]
pub fn quick_row_count(path: &str) -> PyResult<PyRowCountEstimate> {
    let result =
        dataprof::quick_row_count(Path::new(path)).map_err(|e| analysis_error_to_py(&e))?;
    Ok(PyRowCountEstimate { inner: result })
}

/// Analyze a file's structure with a bounded, lightweight pass.
#[pyfunction(signature = (path, max_rows=None))]
pub fn analyze_structure(path: &str, max_rows: Option<usize>) -> PyResult<PyStructureReport> {
    let result = dataprof::analyze_structure(Path::new(path), max_rows)
        .map_err(|e| analysis_error_to_py(&e))?;
    Ok(PyStructureReport { inner: result })
}
