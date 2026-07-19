//! Mapping profiling errors onto Python exceptions, and the shared semantic-hint
//! validation used by the in-memory (DataFrame/Arrow) entry points.
//!
//! The file-based entry points go through `dataprof::Profiler`, which already
//! validates hints and returns a typed error; the DataFrame paths build a report
//! directly, so they call [`validate_report_hints`] to get the same contract.

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use dataprof::{DataProfilerError, ProfileReport, SemanticHints};

/// Map a profiling error to the most appropriate Python exception.
///
/// Mistakes in user input — invalid configuration, semantic hints that cannot
/// bind — surface as `ValueError`; everything else is a `RuntimeError`.
pub(crate) fn analysis_error_to_py(err: &DataProfilerError) -> PyErr {
    match err {
        DataProfilerError::InvalidSemanticHint { .. }
        | DataProfilerError::InvalidConfiguration { .. } => PyValueError::new_err(err.to_string()),
        _ => PyRuntimeError::new_err(format!("Analysis failed: {err}")),
    }
}

/// Enforce the semantic-hint contract against a freshly built report: every
/// hinted column must exist, and no hint may be proven inert over the full data.
pub(crate) fn validate_report_hints(report: &ProfileReport, hints: &SemanticHints) -> PyResult<()> {
    if hints.is_empty() {
        return Ok(());
    }
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
