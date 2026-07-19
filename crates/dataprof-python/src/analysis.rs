use pyo3::prelude::*;
use std::collections::HashMap;
use std::path::Path;

use dataprof::{Profiler, list_patterns as dataprof_list_patterns};

use super::config::PyProfilerConfig;
use super::errors::analysis_error_to_py;
use super::types::PyProfileReport;

/// Analyze a file and return a full profile report.
///
/// Format is auto-detected from the file extension unless overridden
/// via `config.format`. Supports CSV, JSON, JSONL, and Parquet.
///
/// The GIL is released during profiling so that progress callbacks
/// (which re-acquire the GIL) can execute on the profiling thread.
#[pyfunction]
#[pyo3(signature = (path, config=None))]
pub fn analyze_file(
    py: Python<'_>,
    path: &str,
    config: Option<&PyProfilerConfig>,
) -> PyResult<PyProfileReport> {
    let profiler = match config {
        Some(cfg) => cfg.to_profiler(),
        None => Profiler::new(),
    };

    let path = path.to_string();
    let report = py
        .detach(|| profiler.analyze_file(Path::new(&path)))
        .map_err(|e| analysis_error_to_py(&e))?;

    Ok(PyProfileReport::new(report))
}

/// List supported pattern detectors and their metadata.
#[pyfunction]
#[pyo3(signature = (locale=None))]
pub fn list_patterns(
    py: Python<'_>,
    locale: Option<&str>,
) -> PyResult<Vec<HashMap<String, Py<PyAny>>>> {
    dataprof_list_patterns(locale)
        .into_iter()
        .map(|pattern| {
            let mut item = HashMap::new();
            item.insert(
                "name".into(),
                pattern.name.into_pyobject(py)?.unbind().into_any(),
            );
            item.insert(
                "regex".into(),
                pattern.regex.into_pyobject(py)?.unbind().into_any(),
            );
            item.insert(
                "category".into(),
                pattern
                    .category
                    .to_string()
                    .into_pyobject(py)?
                    .unbind()
                    .into_any(),
            );
            item.insert(
                "locale".into(),
                match pattern.locale {
                    Some(locale) => locale.into_pyobject(py)?.unbind().into_any(),
                    None => py.None(),
                },
            );
            item.insert(
                "min_threshold".into(),
                pattern.min_threshold.into_pyobject(py)?.unbind().into_any(),
            );
            Ok(item)
        })
        .collect()
}
