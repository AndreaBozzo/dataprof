use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::path::Path;

#[cfg(feature = "datafusion")]
use crate::engines::DataFusionLoader;
use crate::parsers::json::{JsonParserConfig, analyze_json_file as json_analyze_file_rust};

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
) -> PyResult<crate::types::ProfileReport> {
    let path_obj = Path::new(path);

    if let Some(engine_name) = engine {
        match engine_name.to_lowercase().as_str() {
            "arrow" | "columnar" => crate::api::Profiler::new()
                .engine(crate::api::EngineType::Columnar)
                .analyze_file(path_obj)
                .map_err(|e| PyRuntimeError::new_err(format!("Columnar analysis failed: {}", e))),
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
            _ => crate::api::Profiler::new()
                .analyze_file(path_obj)
                .map_err(|e| PyRuntimeError::new_err(format!("Analysis failed: {}", e))),
        }
    } else {
        crate::api::Profiler::new()
            .analyze_file(path_obj)
            .map_err(|e| PyRuntimeError::new_err(format!("Analysis failed: {}", e)))
    }
}

/// Analyze a JSON file
#[pyfunction]
pub fn analyze_json_file(path: &str) -> PyResult<Vec<PyColumnProfile>> {
    let report = json_analyze_file_rust(Path::new(path), &JsonParserConfig::default())
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to analyze JSON: {}", e)))?;

    Ok(report
        .column_profiles
        .iter()
        .map(PyColumnProfile::from)
        .collect())
}

/// Analyze a JSON file with quality assessment
#[pyfunction]
pub fn analyze_json_with_quality(path: &str) -> PyResult<PyQualityReport> {
    let quality_report = json_analyze_file_rust(Path::new(path), &JsonParserConfig::default())
        .map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to analyze JSON with quality: {}", e))
        })?;

    let py_quality = PyQualityReport::from(&quality_report);

    Ok(py_quality)
}

/// Analyze a Parquet file
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

/// Analyze a Parquet file with quality assessment
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
    let quality_report = analyze_csv_internal(path, None)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to analyze CSV: {}", e)))?;

    // Always return a value to preserve the existing Python API contract.
    // When quality is not computed, return neutral/empty metrics.
    Ok(Some(
        quality_report
            .quality
            .as_ref()
            .map(|q| PyDataQualityMetrics::from(&q.metrics))
            .unwrap_or_else(PyDataQualityMetrics::empty),
    ))
}
