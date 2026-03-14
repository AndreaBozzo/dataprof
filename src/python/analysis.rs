use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::path::Path;

use super::config::PyProfilerConfig;
use super::types::PyProfileReport;

/// Analyze a file and return a full profile report.
///
/// Format is auto-detected from the file extension unless overridden
/// via `config.format`. Supports CSV, JSON, JSONL, and Parquet.
#[pyfunction]
#[pyo3(signature = (path, config=None))]
pub fn analyze_file(path: &str, config: Option<&PyProfilerConfig>) -> PyResult<PyProfileReport> {
    let profiler = match config {
        Some(cfg) => cfg.to_profiler(),
        None => crate::api::Profiler::new(),
    };

    let report = profiler
        .analyze_file(Path::new(path))
        .map_err(|e| PyRuntimeError::new_err(format!("Analysis failed: {}", e)))?;

    Ok(PyProfileReport::new(report))
}
