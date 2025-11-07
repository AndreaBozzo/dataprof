use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use std::path::Path;

use crate::analyze_csv;

use super::types::PyColumnProfile;

/// Analyze CSV and return column profiles as pandas DataFrame (if pandas available)
#[pyfunction]
pub fn analyze_csv_dataframe(py: Python, path: &str) -> PyResult<Py<PyAny>> {
    // Get column profiles
    let profiles = analyze_csv(Path::new(path))
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to analyze CSV: {}", e)))?;

    // Try to import pandas
    let pandas = match py.import("pandas") {
        Ok(pd) => pd,
        Err(_) => {
            return Err(PyRuntimeError::new_err(
                "pandas not available. Install with: pip install pandas",
            ));
        }
    };

    // Create DataFrame data
    let mut data: std::collections::HashMap<&str, Vec<Py<PyAny>>> = std::collections::HashMap::new();
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
            vec.push(py_profile.name.into_pyobject(py)?.into());
        }
        if let Some(vec) = data.get_mut("data_type") {
            vec.push(py_profile.data_type.into_pyobject(py)?.into());
        }
        if let Some(vec) = data.get_mut("total_count") {
            vec.push(py_profile.total_count.into_pyobject(py)?.into());
        }
        if let Some(vec) = data.get_mut("null_count") {
            vec.push(py_profile.null_count.into_pyobject(py)?.into());
        }
        if let Some(vec) = data.get_mut("null_percentage") {
            vec.push(py_profile.null_percentage.into_pyobject(py)?.into());
        }
        if let Some(vec) = data.get_mut("unique_count") {
            vec.push(py_profile.unique_count.into_pyobject(py)?.into());
        }
        if let Some(vec) = data.get_mut("uniqueness_ratio") {
            vec.push(py_profile.uniqueness_ratio.into_pyobject(py)?.into());
        }
    }

    // Create DataFrame
    let df = pandas.call_method1("DataFrame", (data,))?;
    Ok(df.into())
}
