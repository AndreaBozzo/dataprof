use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::path::Path;

use crate::analysis::MlReadinessEngine;
use crate::{analyze_csv, analyze_csv_robust};

use super::types::{PyColumnProfile, PyFeatureAnalysis};

/// Analyze CSV and return column profiles as pandas DataFrame (if pandas available)
#[pyfunction]
pub fn analyze_csv_dataframe(py: Python, path: &str) -> PyResult<PyObject> {
    // Get column profiles
    let profiles = analyze_csv(Path::new(path))
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to analyze CSV: {}", e)))?;

    // Try to import pandas
    let pandas = match py.import_bound("pandas") {
        Ok(pd) => pd,
        Err(_) => {
            return Err(PyRuntimeError::new_err(
                "pandas not available. Install with: pip install pandas",
            ));
        }
    };

    // Create DataFrame data
    let mut data: std::collections::HashMap<&str, Vec<PyObject>> = std::collections::HashMap::new();
    data.insert("column_name", Vec::new());
    data.insert("data_type", Vec::new());
    data.insert("total_count", Vec::new());
    data.insert("null_count", Vec::new());
    data.insert("null_percentage", Vec::new());
    data.insert("unique_count", Vec::new());
    data.insert("uniqueness_ratio", Vec::new());

    for profile in &profiles {
        let py_profile = PyColumnProfile::from(profile);
        if let Some(vec) = data.get_mut("column_name") {
            vec.push(py_profile.name.into_py(py));
        }
        if let Some(vec) = data.get_mut("data_type") {
            vec.push(py_profile.data_type.into_py(py));
        }
        if let Some(vec) = data.get_mut("total_count") {
            vec.push(py_profile.total_count.into_py(py));
        }
        if let Some(vec) = data.get_mut("null_count") {
            vec.push(py_profile.null_count.into_py(py));
        }
        if let Some(vec) = data.get_mut("null_percentage") {
            vec.push(py_profile.null_percentage.into_py(py));
        }
        if let Some(vec) = data.get_mut("unique_count") {
            vec.push(py_profile.unique_count.into_py(py));
        }
        if let Some(vec) = data.get_mut("uniqueness_ratio") {
            vec.push(py_profile.uniqueness_ratio.into_py(py));
        }
    }

    // Create DataFrame
    let df = pandas.call_method1("DataFrame", (data,))?;
    Ok(df.into())
}

/// Get ML feature analysis as pandas DataFrame (if pandas available)
#[pyfunction]
pub fn feature_analysis_dataframe(py: Python, path: &str) -> PyResult<PyObject> {
    // Get ML readiness score
    let quality_report = analyze_csv_robust(Path::new(path))
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to analyze CSV: {}", e)))?;

    let ml_engine = MlReadinessEngine::new();
    let ml_score = ml_engine
        .calculate_ml_score(&quality_report)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to calculate ML score: {}", e)))?;

    // Try to import pandas
    let pandas = match py.import_bound("pandas") {
        Ok(pd) => pd,
        Err(_) => {
            return Err(PyRuntimeError::new_err(
                "pandas not available. Install with: pip install pandas",
            ));
        }
    };

    // Create DataFrame data
    let mut data: std::collections::HashMap<&str, Vec<PyObject>> = std::collections::HashMap::new();
    data.insert("column_name", Vec::new());
    data.insert("ml_suitability", Vec::new());
    data.insert("feature_type", Vec::new());
    data.insert("importance_potential", Vec::new());
    data.insert("encoding_suggestions", Vec::new());
    data.insert("potential_issues", Vec::new());

    for feature in &ml_score.feature_analysis {
        let py_feature = PyFeatureAnalysis::from(feature);
        if let Some(vec) = data.get_mut("column_name") {
            vec.push(py_feature.column_name.into_py(py));
        }
        if let Some(vec) = data.get_mut("ml_suitability") {
            vec.push(py_feature.ml_suitability.into_py(py));
        }
        if let Some(vec) = data.get_mut("feature_type") {
            vec.push(py_feature.feature_type.into_py(py));
        }
        if let Some(vec) = data.get_mut("importance_potential") {
            vec.push(py_feature.feature_importance_potential.into_py(py));
        }
        if let Some(vec) = data.get_mut("encoding_suggestions") {
            vec.push(py_feature.encoding_suggestions.join(", ").into_py(py));
        }
        if let Some(vec) = data.get_mut("potential_issues") {
            vec.push(py_feature.potential_issues.join(", ").into_py(py));
        }
    }

    // Create DataFrame
    let df = pandas.call_method1("DataFrame", (data,))?;
    Ok(df.into())
}
