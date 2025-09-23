use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::path::Path;

use crate::analysis::MlReadinessEngine;
use crate::{analyze_csv, analyze_csv_robust, analyze_json};

use super::types::{PyColumnProfile, PyMlReadinessScore, PyQualityReport};

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

/// Comprehensive analysis combining data quality and ML readiness
#[pyfunction]
pub fn analyze_csv_for_ml(path: &str) -> PyResult<(PyQualityReport, PyMlReadinessScore)> {
    // Get quality report
    let quality_report = analyze_csv_robust(Path::new(path))
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to analyze CSV: {}", e)))?;

    // Calculate ML readiness
    let ml_engine = MlReadinessEngine::new();
    let ml_score = ml_engine
        .calculate_ml_score(&quality_report)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to calculate ML score: {}", e)))?;

    Ok((
        PyQualityReport::from(&quality_report),
        PyMlReadinessScore::from(&ml_score),
    ))
}
