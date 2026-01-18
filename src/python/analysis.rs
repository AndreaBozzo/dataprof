use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::path::Path;

use crate::analyze_json_with_quality as analyze_json_quality_rust;
use crate::engines::AdaptiveProfiler;
#[cfg(feature = "datafusion")]
use crate::engines::DataFusionLoader;
use crate::{analyze_csv_robust, analyze_json};

#[cfg(feature = "parquet")]
use crate::analyze_parquet_with_quality as analyze_parquet_quality_rust;

use super::types::{PyColumnProfile, PyDataQualityMetrics, PyQualityReport};

/// Analyze a single CSV file
#[pyfunction]
#[pyo3(signature = (path, engine=None))]
pub fn analyze_csv_file(path: &str, engine: Option<String>) -> PyResult<Vec<PyColumnProfile>> {
    let report = analyze_csv_internal(path, engine)?;
    Ok(report
        .column_profiles
        .iter()
        .map(PyColumnProfile::from)
        .collect())
}

/// Analyze a single CSV file with quality assessment
#[pyfunction]
#[pyo3(signature = (path, engine=None))]
pub fn analyze_csv_with_quality(path: &str, engine: Option<String>) -> PyResult<PyQualityReport> {
    let report = analyze_csv_internal(path, engine)?;
    Ok(PyQualityReport::from(&report))
}

fn analyze_csv_internal(
    path: &str,
    engine: Option<String>,
) -> PyResult<crate::types::QualityReport> {
    let path_obj = Path::new(path);

    if let Some(engine_name) = engine {
        match engine_name.to_lowercase().as_str() {
            "arrow" => {
                let profiler = crate::engines::columnar::ArrowProfiler::new();
                profiler
                    .analyze_csv_file(path_obj)
                    .map_err(|e| PyRuntimeError::new_err(format!("Arrow analysis failed: {}", e)))
            }
            #[cfg(feature = "datafusion")]
            "datafusion" => {
                // Create runtime for async DataFusion execution
                let rt = tokio::runtime::Runtime::new().map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to create runtime: {}", e))
                })?;

                rt.block_on(async {
                    let loader = DataFusionLoader::new();
                    let table_name = "analysis_target";

                    // Register the CSV file as a temporary table
                    loader
                        .register_csv(table_name, path)
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to register CSV: {}", e))?;

                    // Profile the table content
                    let query = format!("SELECT * FROM {}", table_name);
                    loader.profile_query(&query).await
                })
                .map_err(|e| PyRuntimeError::new_err(format!("DataFusion analysis failed: {}", e)))
            }
            "auto" | "adaptive" => {
                let profiler = AdaptiveProfiler::new();
                profiler.analyze_file(path_obj).map_err(|e| {
                    PyRuntimeError::new_err(format!("Adaptive analysis failed: {}", e))
                })
            }
            _ => {
                // Fallback to standard robust analysis for unknown engines (or maybe raise error?)
                // For backward compatibility, maybe just log warning and use robust?
                // Or better, standard logic:
                analyze_csv_robust(path_obj)
                    .map_err(|e| PyRuntimeError::new_err(format!("Analysis failed: {}", e)))
            }
        }
    } else {
        // Default behavior - currently robust csv
        analyze_csv_robust(path_obj)
            .map_err(|e| PyRuntimeError::new_err(format!("Analysis failed: {}", e)))
    }
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
