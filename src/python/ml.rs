use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::path::Path;

use crate::analysis::MlReadinessEngine;
use crate::analyze_csv_robust;

use super::types::PyMlReadinessScore;

/// Calculate ML readiness score for a CSV file
#[pyfunction]
pub fn ml_readiness_score(path: &str) -> PyResult<PyMlReadinessScore> {
    // First get the quality report
    let quality_report = analyze_csv_robust(Path::new(path))
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to analyze CSV: {}", e)))?;

    // Calculate ML readiness using the engine
    let ml_engine = MlReadinessEngine::new();
    let ml_score = ml_engine
        .calculate_ml_score(&quality_report)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to calculate ML score: {}", e)))?;

    Ok(PyMlReadinessScore::from(&ml_score))
}

/// Context manager for ML analysis with resource tracking
#[pyclass]
pub struct PyMlAnalyzer {
    temp_files: Vec<String>,
    ml_results: Vec<PyMlReadinessScore>,
    processing_stats: std::collections::HashMap<String, f64>,
}

#[pymethods]
impl PyMlAnalyzer {
    #[new]
    fn new() -> Self {
        PyMlAnalyzer {
            temp_files: Vec::new(),
            ml_results: Vec::new(),
            processing_stats: std::collections::HashMap::new(),
        }
    }

    /// Enter context manager
    fn __enter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    /// Exit context manager with cleanup
    fn __exit__(
        &mut self,
        _exc_type: Option<PyObject>,
        _exc_value: Option<PyObject>,
        _traceback: Option<PyObject>,
    ) -> PyResult<bool> {
        // Clean up temporary files
        for temp_file in &self.temp_files {
            let _ = std::fs::remove_file(temp_file);
        }
        self.temp_files.clear();
        self.ml_results.clear();
        self.processing_stats.clear();
        Ok(false)
    }

    /// Analyze ML readiness with timing
    fn analyze_ml(&mut self, path: &str) -> PyResult<PyMlReadinessScore> {
        let start_time = std::time::Instant::now();

        let result = ml_readiness_score(path)?;

        let duration = start_time.elapsed().as_secs_f64();
        self.processing_stats.insert(path.to_string(), duration);
        self.ml_results.push(result.clone());

        Ok(result)
    }

    /// Add temporary file for cleanup
    fn add_temp_file(&mut self, path: String) {
        self.temp_files.push(path);
    }

    /// Get processing statistics
    fn get_stats(&self, py: Python) -> PyResult<PyObject> {
        Ok(self.processing_stats.clone().into_py(py))
    }

    /// Get all ML results
    fn get_ml_results(&self, py: Python) -> PyResult<PyObject> {
        Ok(self.ml_results.clone().into_py(py))
    }

    /// Get summary statistics
    fn get_summary(&self, py: Python) -> PyResult<PyObject> {
        if self.ml_results.is_empty() {
            return Ok(py.None());
        }

        let total_files = self.ml_results.len();
        let avg_score =
            self.ml_results.iter().map(|r| r.overall_score).sum::<f64>() / total_files as f64;

        let ready_files = self
            .ml_results
            .iter()
            .filter(|r| r.readiness_level == "ready")
            .count();

        let total_time: f64 = self.processing_stats.values().sum();
        let avg_time = if total_files > 0 {
            total_time / total_files as f64
        } else {
            0.0
        };

        let mut summary = std::collections::HashMap::new();
        summary.insert("total_files", total_files.into_py(py));
        summary.insert("average_score", avg_score.into_py(py));
        summary.insert("ready_files", ready_files.into_py(py));
        summary.insert(
            "ready_percentage",
            ((ready_files as f64 / total_files as f64) * 100.0).into_py(py),
        );
        summary.insert("total_processing_time", total_time.into_py(py));
        summary.insert("average_processing_time", avg_time.into_py(py));

        Ok(summary.into_py(py))
    }
}
