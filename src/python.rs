#![allow(clippy::useless_conversion)]

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use std::path::Path;

use crate::core::batch::{BatchProcessor, BatchResult};
use crate::types::{ColumnProfile, DataType, QualityIssue, QualityReport};
use crate::{analyze_csv, analyze_csv_robust, analyze_json};

/// Python wrapper for ColumnProfile
#[pyclass]
#[derive(Clone)]
pub struct PyColumnProfile {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub data_type: String,
    #[pyo3(get)]
    pub total_count: usize,
    #[pyo3(get)]
    pub null_count: usize,
    #[pyo3(get)]
    pub unique_count: Option<usize>,
    #[pyo3(get)]
    pub null_percentage: f64,
    #[pyo3(get)]
    pub uniqueness_ratio: f64,
}

impl From<&ColumnProfile> for PyColumnProfile {
    fn from(profile: &ColumnProfile) -> Self {
        let null_percentage = if profile.total_count > 0 {
            (profile.null_count as f64 / profile.total_count as f64) * 100.0
        } else {
            0.0
        };

        let uniqueness_ratio = if let Some(unique) = profile.unique_count {
            if profile.total_count > 0 {
                unique as f64 / profile.total_count as f64
            } else {
                0.0
            }
        } else {
            0.0
        };

        Self {
            name: profile.name.clone(),
            data_type: match profile.data_type {
                DataType::Integer => "integer".to_string(),
                DataType::Float => "float".to_string(),
                DataType::String => "string".to_string(),
                DataType::Date => "date".to_string(),
            },
            total_count: profile.total_count,
            null_count: profile.null_count,
            unique_count: profile.unique_count,
            null_percentage,
            uniqueness_ratio,
        }
    }
}

/// Python wrapper for QualityIssue
#[pyclass]
#[derive(Clone)]
pub struct PyQualityIssue {
    #[pyo3(get)]
    pub issue_type: String,
    #[pyo3(get)]
    pub column: String,
    #[pyo3(get)]
    pub severity: String,
    #[pyo3(get)]
    pub count: Option<usize>,
    #[pyo3(get)]
    pub percentage: Option<f64>,
    #[pyo3(get)]
    pub description: String,
}

impl From<&QualityIssue> for PyQualityIssue {
    fn from(issue: &QualityIssue) -> Self {
        match issue {
            QualityIssue::NullValues {
                column,
                count,
                percentage,
            } => Self {
                issue_type: "null_values".to_string(),
                column: column.to_string(),
                severity: "medium".to_string(),
                count: Some(*count),
                percentage: Some(*percentage),
                description: format!(
                    "{} null values ({}%) in column '{}'",
                    count, percentage, column
                ),
            },
            QualityIssue::Duplicates { column, count } => Self {
                issue_type: "duplicates".to_string(),
                column: column.to_string(),
                severity: "low".to_string(),
                count: Some(*count),
                percentage: None,
                description: format!("{} duplicate values in column '{}'", count, column),
            },
            QualityIssue::Outliers {
                column,
                values,
                threshold,
            } => Self {
                issue_type: "outliers".to_string(),
                column: column.to_string(),
                severity: "medium".to_string(),
                count: Some(values.len()),
                percentage: None,
                description: format!(
                    "{} outlier values in column '{}' (threshold: {}): {:?}",
                    values.len(),
                    column,
                    threshold,
                    values
                ),
            },
            QualityIssue::MixedDateFormats { column, formats } => Self {
                issue_type: "mixed_date_formats".to_string(),
                column: column.to_string(),
                severity: "high".to_string(),
                count: Some(formats.len()),
                percentage: None,
                description: format!("Mixed date formats in column '{}': {:?}", column, formats),
            },
            QualityIssue::MixedTypes { column, types } => Self {
                issue_type: "mixed_types".to_string(),
                column: column.to_string(),
                severity: "high".to_string(),
                count: Some(types.len()),
                percentage: None,
                description: format!("Mixed data types in column '{}': {:?}", column, types),
            },
        }
    }
}

/// Python wrapper for QualityReport
#[pyclass]
#[derive(Clone)]
pub struct PyQualityReport {
    #[pyo3(get)]
    pub file_path: String,
    #[pyo3(get)]
    pub total_rows: Option<usize>,
    #[pyo3(get)]
    pub total_columns: usize,
    #[pyo3(get)]
    pub column_profiles: Vec<PyColumnProfile>,
    #[pyo3(get)]
    pub issues: Vec<PyQualityIssue>,
    #[pyo3(get)]
    pub rows_scanned: usize,
    #[pyo3(get)]
    pub sampling_ratio: f64,
    #[pyo3(get)]
    pub scan_time_ms: u128,
}

impl From<&QualityReport> for PyQualityReport {
    fn from(report: &QualityReport) -> Self {
        Self {
            file_path: report.file_info.path.clone(),
            total_rows: report.file_info.total_rows,
            total_columns: report.file_info.total_columns,
            column_profiles: report
                .column_profiles
                .iter()
                .map(PyColumnProfile::from)
                .collect(),
            issues: report.issues.iter().map(PyQualityIssue::from).collect(),
            rows_scanned: report.scan_info.rows_scanned,
            sampling_ratio: report.scan_info.sampling_ratio,
            scan_time_ms: report.scan_info.scan_time_ms,
        }
    }
}

#[pymethods]
impl PyQualityReport {
    /// Calculate overall quality score (0-100)
    fn quality_score(&self) -> PyResult<f64> {
        if self.issues.is_empty() {
            return Ok(100.0);
        }

        let mut score: f64 = 100.0;

        for issue in &self.issues {
            let penalty = match issue.issue_type.as_str() {
                "mixed_date_formats" => 20.0,
                "null_values" => {
                    if let Some(percentage) = issue.percentage {
                        if percentage > 50.0 {
                            20.0
                        } else if percentage > 20.0 {
                            15.0
                        } else {
                            10.0
                        }
                    } else {
                        10.0
                    }
                }
                "outlier_values" => 15.0,
                "invalid_email_format" => 10.0,
                "duplicate_values" => 5.0,
                "inconsistent_casing" => 3.0,
                _ => 5.0,
            };
            score -= penalty;
        }

        Ok(score.max(0.0))
    }

    /// Get issues by severity
    fn issues_by_severity(&self, severity: &str) -> Vec<PyQualityIssue> {
        self.issues
            .iter()
            .filter(|issue| issue.severity == severity)
            .cloned()
            .collect()
    }
}

/// Python wrapper for BatchResult
#[pyclass]
#[derive(Clone)]
pub struct PyBatchResult {
    #[pyo3(get)]
    pub processed_files: usize,
    #[pyo3(get)]
    pub failed_files: usize,
    #[pyo3(get)]
    pub total_duration_secs: f64,
    #[pyo3(get)]
    pub total_quality_issues: usize,
    #[pyo3(get)]
    pub average_quality_score: f64,
}

impl From<&BatchResult> for PyBatchResult {
    fn from(result: &BatchResult) -> Self {
        Self {
            processed_files: result.summary.successful,
            failed_files: result.summary.failed,
            total_duration_secs: result.summary.processing_time_seconds,
            total_quality_issues: result.summary.total_issues,
            average_quality_score: result.summary.average_quality_score,
        }
    }
}

/// Analyze a single CSV file
#[pyfunction]
fn analyze_csv_file(path: &str) -> PyResult<Vec<PyColumnProfile>> {
    let profiles = analyze_csv(Path::new(path))
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to analyze CSV: {}", e)))?;

    Ok(profiles.iter().map(PyColumnProfile::from).collect())
}

/// Analyze a single CSV file with quality assessment
#[pyfunction]
fn analyze_csv_with_quality(path: &str) -> PyResult<PyQualityReport> {
    let quality_report = analyze_csv_robust(Path::new(path)).map_err(|e| {
        PyRuntimeError::new_err(format!("Failed to analyze CSV with quality: {}", e))
    })?;

    let py_quality = PyQualityReport::from(&quality_report);

    Ok(py_quality)
}

/// Analyze a JSON file
#[pyfunction]
fn analyze_json_file(path: &str) -> PyResult<Vec<PyColumnProfile>> {
    let profiles = analyze_json(Path::new(path))
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to analyze JSON: {}", e)))?;

    Ok(profiles.iter().map(PyColumnProfile::from).collect())
}

/// Batch process multiple files using glob pattern
#[pyfunction]
#[pyo3(signature = (pattern, parallel=None, max_concurrent=None))]
fn batch_analyze_glob(
    pattern: &str,
    parallel: Option<bool>,
    max_concurrent: Option<usize>,
) -> PyResult<PyBatchResult> {
    use crate::core::batch::BatchConfig;

    let config = BatchConfig {
        parallel: parallel.unwrap_or(true),
        max_concurrent: max_concurrent.unwrap_or_else(num_cpus::get),
        recursive: false, // Not applicable for glob patterns
        extensions: vec!["csv".to_string(), "json".to_string(), "jsonl".to_string()],
        exclude_patterns: vec!["**/.*".to_string(), "**/*tmp*".to_string()],
    };

    let processor = BatchProcessor::with_config(config);
    let result = processor
        .process_glob(pattern)
        .map_err(|e| PyRuntimeError::new_err(format!("Batch processing failed: {}", e)))?;

    Ok(PyBatchResult::from(&result))
}

/// Batch process all files in a directory
#[pyfunction]
#[pyo3(signature = (directory, recursive=None, parallel=None, max_concurrent=None))]
fn batch_analyze_directory(
    directory: &str,
    recursive: Option<bool>,
    parallel: Option<bool>,
    max_concurrent: Option<usize>,
) -> PyResult<PyBatchResult> {
    use crate::core::batch::BatchConfig;

    let config = BatchConfig {
        parallel: parallel.unwrap_or(true),
        max_concurrent: max_concurrent.unwrap_or_else(num_cpus::get),
        recursive: recursive.unwrap_or(false),
        extensions: vec!["csv".to_string(), "json".to_string(), "jsonl".to_string()],
        exclude_patterns: vec!["**/.*".to_string(), "**/*tmp*".to_string()],
    };

    let processor = BatchProcessor::with_config(config);
    let result = processor
        .process_directory(std::path::Path::new(directory))
        .map_err(|e| PyRuntimeError::new_err(format!("Batch processing failed: {}", e)))?;

    Ok(PyBatchResult::from(&result))
}

/// Python module definition
#[pymodule]
fn dataprof(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyColumnProfile>()?;
    m.add_class::<PyQualityReport>()?;
    m.add_class::<PyQualityIssue>()?;
    m.add_class::<PyBatchResult>()?;

    // Single file analysis
    m.add_function(wrap_pyfunction!(analyze_csv_file, m)?)?;
    m.add_function(wrap_pyfunction!(analyze_csv_with_quality, m)?)?;
    m.add_function(wrap_pyfunction!(analyze_json_file, m)?)?;

    // Batch processing
    m.add_function(wrap_pyfunction!(batch_analyze_glob, m)?)?;
    m.add_function(wrap_pyfunction!(batch_analyze_directory, m)?)?;

    Ok(())
}
