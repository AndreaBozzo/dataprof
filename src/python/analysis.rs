use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::path::Path;

use crate::{analyze_csv, analyze_csv_robust, analyze_json};

use super::types::{PyColumnProfile, PyDataQualityMetrics, PyQualityReport};

/// Analyze a single CSV file
#[pyfunction]
pub fn analyze_csv_file(path: &str) -> PyResult<Vec<PyColumnProfile>> {
    let profiles = analyze_csv(Path::new(path))
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to analyze CSV: {}", e)))?;

    Ok(profiles.iter().map(PyColumnProfile::from).collect())
}

/// Analyze a single CSV file with quality assessment
#[pyfunction]
pub fn analyze_csv_with_quality(path: &str) -> PyResult<PyQualityReport> {
    let quality_report = analyze_csv_robust(Path::new(path)).map_err(|e| {
        PyRuntimeError::new_err(format!("Failed to analyze CSV with quality: {}", e))
    })?;

    let py_quality = PyQualityReport::from(&quality_report);

    Ok(py_quality)
}

/// Analyze a JSON file
#[pyfunction]
pub fn analyze_json_file(path: &str) -> PyResult<Vec<PyColumnProfile>> {
    let profiles = analyze_json(Path::new(path))
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to analyze JSON: {}", e)))?;

    Ok(profiles.iter().map(PyColumnProfile::from).collect())
}

/// Calculate data quality metrics for a CSV file
#[pyfunction]
pub fn calculate_data_quality_metrics(path: &str) -> PyResult<Option<PyDataQualityMetrics>> {
    let quality_report = analyze_csv_robust(Path::new(path))
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to analyze CSV: {}", e)))?;

    match quality_report.data_quality_metrics {
        Some(metrics) => Ok(Some(PyDataQualityMetrics::from(&metrics))),
        None => Ok(None),
    }
}
