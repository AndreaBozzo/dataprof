use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::path::Path;

use crate::analyze_json_with_quality as analyze_json_quality_rust;
use crate::{analyze_csv, analyze_csv_robust, analyze_json};

#[cfg(feature = "parquet")]
use crate::analyze_parquet_with_quality as analyze_parquet_quality_rust;

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

/// Analyze a JSON file with quality assessment
#[pyfunction]
pub fn analyze_json_with_quality(path: &str) -> PyResult<PyQualityReport> {
    let quality_report = analyze_json_quality_rust(Path::new(path)).map_err(|e| {
        PyRuntimeError::new_err(format!("Failed to analyze JSON with quality: {}", e))
    })?;

    let py_quality = PyQualityReport::from(&quality_report);

    Ok(py_quality)
}

/// Analyze a Parquet file (only available with parquet feature)
#[cfg(feature = "parquet")]
#[pyfunction]
pub fn analyze_parquet_file(path: &str) -> PyResult<Vec<PyColumnProfile>> {
    let quality_report = analyze_parquet_quality_rust(Path::new(path))
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to analyze Parquet: {}", e)))?;

    Ok(quality_report
        .column_profiles
        .iter()
        .map(PyColumnProfile::from)
        .collect())
}

/// Analyze a Parquet file with quality assessment (only available with parquet feature)
#[cfg(feature = "parquet")]
#[pyfunction]
pub fn analyze_parquet_with_quality_py(path: &str) -> PyResult<PyQualityReport> {
    let quality_report = analyze_parquet_quality_rust(Path::new(path)).map_err(|e| {
        PyRuntimeError::new_err(format!("Failed to analyze Parquet with quality: {}", e))
    })?;

    let py_quality = PyQualityReport::from(&quality_report);

    Ok(py_quality)
}

/// Calculate data quality metrics for a CSV file
#[pyfunction]
pub fn calculate_data_quality_metrics(path: &str) -> PyResult<Option<PyDataQualityMetrics>> {
    let quality_report = analyze_csv_robust(Path::new(path))
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to analyze CSV: {}", e)))?;

    Ok(Some(PyDataQualityMetrics::from(
        &quality_report.data_quality_metrics,
    )))
}
