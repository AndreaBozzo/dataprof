use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::core::batch::{BatchConfig, BatchProcessor};

#[cfg(feature = "parquet")]
use super::analysis::analyze_parquet_file;
use super::analysis::{analyze_csv_file, analyze_json_file};
use super::types::PyBatchResult;

/// Batch process multiple files using glob pattern
#[pyfunction]
#[pyo3(signature = (pattern, parallel=None, max_concurrent=None, html_output=None))]
pub fn batch_analyze_glob(
    pattern: &str,
    parallel: Option<bool>,
    max_concurrent: Option<usize>,
    html_output: Option<String>,
) -> PyResult<PyBatchResult> {
    let config = BatchConfig {
        parallel: parallel.unwrap_or(true),
        max_concurrent: max_concurrent.unwrap_or_else(num_cpus::get),
        recursive: false, // Not applicable for glob patterns
        extensions: vec![
            "csv".to_string(),
            "json".to_string(),
            "jsonl".to_string(),
            "parquet".to_string(),
        ],
        exclude_patterns: vec!["**/.*".to_string(), "**/*tmp*".to_string()],
        html_output: html_output.map(std::path::PathBuf::from),
    };

    let processor = BatchProcessor::with_config(config);
    let result = processor
        .process_glob(pattern)
        .map_err(|e| PyRuntimeError::new_err(format!("Batch processing failed: {}", e)))?;

    Ok(PyBatchResult::from(&result))
}

/// Batch process all files in a directory
#[pyfunction]
#[pyo3(signature = (directory, recursive=None, parallel=None, max_concurrent=None, html_output=None))]
pub fn batch_analyze_directory(
    directory: &str,
    recursive: Option<bool>,
    parallel: Option<bool>,
    max_concurrent: Option<usize>,
    html_output: Option<String>,
) -> PyResult<PyBatchResult> {
    let config = BatchConfig {
        parallel: parallel.unwrap_or(true),
        max_concurrent: max_concurrent.unwrap_or_else(num_cpus::get),
        recursive: recursive.unwrap_or(false),
        extensions: vec![
            "csv".to_string(),
            "json".to_string(),
            "jsonl".to_string(),
            "parquet".to_string(),
        ],
        exclude_patterns: vec!["**/.*".to_string(), "**/*tmp*".to_string()],
        html_output: html_output.map(std::path::PathBuf::from),
    };

    let processor = BatchProcessor::with_config(config);
    let result = processor
        .process_directory(std::path::Path::new(directory))
        .map_err(|e| PyRuntimeError::new_err(format!("Batch processing failed: {}", e)))?;

    Ok(PyBatchResult::from(&result))
}

/// Context manager for batch analysis with automatic cleanup
#[pyclass]
pub struct PyBatchAnalyzer {
    temp_files: Vec<String>,
    results: Vec<Py<PyAny>>,
}

#[pymethods]
impl PyBatchAnalyzer {
    #[new]
    fn new() -> Self {
        PyBatchAnalyzer {
            temp_files: Vec::new(),
            results: Vec::new(),
        }
    }

    /// Enter context manager
    fn __enter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    /// Exit context manager with cleanup
    fn __exit__(
        &mut self,
        _exc_type: Option<Py<PyAny>>,
        _exc_value: Option<Py<PyAny>>,
        _traceback: Option<Py<PyAny>>,
    ) -> PyResult<bool> {
        // Clean up temporary files
        for temp_file in &self.temp_files {
            let _ = std::fs::remove_file(temp_file);
        }
        self.temp_files.clear();
        self.results.clear();
        Ok(false) // Don't suppress exceptions
    }

    /// Add a file to analysis queue (detects format automatically)
    fn add_file(&mut self, py: Python, path: &str) -> PyResult<()> {
        // Detect file extension
        let path_obj = std::path::Path::new(path);
        let ext = path_obj
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();

        // Route to appropriate analyzer based on extension
        let result = match ext.as_str() {
            "json" | "jsonl" => analyze_json_file(path),
            #[cfg(feature = "parquet")]
            "parquet" => analyze_parquet_file(path),
            #[cfg(not(feature = "parquet"))]
            "parquet" => Err(PyRuntimeError::new_err(
                "Parquet support not enabled. Rebuild with --features parquet",
            )),
            "csv" => analyze_csv_file(path),
            _ => analyze_csv_file(path), // Default to CSV for unknown extensions
        }?;

        self.results.push(result.into_pyobject(py)?.into());
        Ok(())
    }

    /// Add a temporary file that needs cleanup
    fn add_temp_file(&mut self, path: String) {
        self.temp_files.push(path);
    }

    /// Get all analysis results
    fn get_results(&self, py: Python) -> PyResult<Py<PyAny>> {
        let results_ref: Vec<&Py<PyAny>> = self.results.iter().collect();
        Ok(results_ref.into_pyobject(py)?.into())
    }

    /// Analyze multiple files in batch (detects format automatically)
    fn analyze_batch(&mut self, py: Python, paths: Vec<String>) -> PyResult<Py<PyAny>> {
        let mut batch_results: Vec<Py<PyAny>> = Vec::new();

        for path in paths {
            // Detect file extension
            let path_obj = std::path::Path::new(&path);
            let ext = path_obj
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("")
                .to_lowercase();

            // Route to appropriate analyzer based on extension
            let result = match ext.as_str() {
                "json" | "jsonl" => analyze_json_file(&path),
                #[cfg(feature = "parquet")]
                "parquet" => analyze_parquet_file(&path),
                #[cfg(not(feature = "parquet"))]
                "parquet" => Err(PyRuntimeError::new_err(
                    "Parquet support not enabled. Rebuild with --features parquet",
                )),
                "csv" => analyze_csv_file(&path),
                _ => analyze_csv_file(&path), // Default to CSV for unknown extensions
            };

            match result {
                Ok(profiles) => {
                    batch_results.push(profiles.into_pyobject(py)?.into());
                }
                Err(e) => {
                    return Err(PyRuntimeError::new_err(format!(
                        "Failed to analyze {}: {}",
                        path, e
                    )));
                }
            }
        }

        for result in &batch_results {
            self.results.push(result.clone_ref(py));
        }
        Ok(batch_results.into_pyobject(py)?.into())
    }
}
