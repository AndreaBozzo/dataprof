//! Mapping profiling errors onto Python exceptions, and the shared semantic-hint
//! validation used by the in-memory (DataFrame/Arrow) entry points.
//!
//! The file-based entry points go through `dataprof::Profiler`, which already
//! validates hints and returns a typed error; the DataFrame paths build a report
//! directly, so they call [`validate_report_hints`] to get the same contract.

use pyo3::exceptions::{
    PyFileNotFoundError, PyIOError, PyPermissionError, PyRuntimeError, PyValueError,
};
use pyo3::prelude::*;

use dataprof::{DataProfilerError, ProfileReport, SemanticHints};

/// Map a profiling error to the most appropriate Python exception.
///
/// The category drives the exception type so callers can `except` on the
/// idiomatic Python class instead of string-matching a `RuntimeError`:
///   * bad user input (config, semantic hints, unsupported format) → `ValueError`
///   * a missing file → `FileNotFoundError`
///   * permission / other I/O trouble → `PermissionError` / `IOError`
///   * everything else → `RuntimeError`
///
/// The `DataProfilerError` `Display` already carries the actionable suggestion,
/// so the message is passed through verbatim rather than re-wrapped.
pub(crate) fn analysis_error_to_py(err: &DataProfilerError) -> PyErr {
    let message = err.to_string();
    match err {
        DataProfilerError::InvalidSemanticHint { .. }
        | DataProfilerError::InvalidConfiguration { .. }
        | DataProfilerError::UnsupportedFormat { .. } => PyValueError::new_err(message),
        DataProfilerError::FileNotFound { .. } => PyFileNotFoundError::new_err(message),
        DataProfilerError::IoError { .. } => {
            if message.contains("Permission denied") {
                PyPermissionError::new_err(message)
            } else {
                PyIOError::new_err(message)
            }
        }
        _ => PyRuntimeError::new_err(format!("Analysis failed: {message}")),
    }
}

/// Enforce the semantic-hint contract against a freshly built report: every
/// hinted column must exist, and no hint may be proven inert over the full data.
pub(crate) fn validate_report_hints(
    report: &ProfileReport,
    hints: &SemanticHints,
    quality_requested: bool,
) -> PyResult<()> {
    if hints.is_empty() {
        return Ok(());
    }
    hints
        .validate_quality_usage(quality_requested)
        .map_err(|e| analysis_error_to_py(&e))?;
    let names: Vec<&str> = report
        .column_profiles
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    hints
        .validate_names(&names)
        .map_err(|e| analysis_error_to_py(&e))?;
    hints
        .validate_bindings(&report.semantic_hint_bindings)
        .map_err(|e| analysis_error_to_py(&e))?;
    Ok(())
}
