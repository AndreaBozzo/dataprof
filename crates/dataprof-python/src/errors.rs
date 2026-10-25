//! Mapping profiling errors onto Python exceptions, and the shared semantic-hint
//! validation used by the in-memory (DataFrame/Arrow) entry points.
//!
//! The file-based entry points go through `dataprof::Profiler`, which already
//! validates hints and returns a typed error; the DataFrame paths build a report
//! directly, so they call the internal `validate_report_hints` helper to get
//! the same contract.

use pyo3::exceptions::{
    PyFileNotFoundError, PyIOError, PyPermissionError, PyRuntimeError, PyValueError,
};
use pyo3::prelude::*;

use dataprof::{DataProfilerError, ProfileReport, SemanticHints};

/// Map a profiling error to the most appropriate Python exception.
///
/// The category drives the exception type so callers can `except` on the
/// idiomatic Python class instead of string-matching a `RuntimeError`:
///   * bad user input (config, semantic hints, unsupported format, a malformed
///     or unsupported database connection string) → `ValueError`
///   * malformed data — CSV *and* JSON alike → `ValueError`
///   * a missing file → `FileNotFoundError`
///   * permission / other I/O trouble → `PermissionError` / `IOError`
///   * database connection failures → `OSError` (not necessarily retryable)
///   * everything else → `RuntimeError`
///
/// The `DataProfilerError` `Display` already carries the actionable suggestion,
/// so the message is passed through verbatim rather than re-wrapped.
///
/// Any retained cause chain (see `DataProfilerError`'s `ErrorSource`) is
/// attached as the exception's `__cause__`, so `raise ... from ...` context
/// shows the originating decoder or OS error in the traceback instead of it
/// being visible only from Rust.
pub(crate) fn analysis_error_to_py(err: &DataProfilerError) -> PyErr {
    let py_err = analysis_error_to_py_uncaused(err);
    match rust_cause_chain(err) {
        Some(cause) => Python::attach(|py| {
            py_err.set_cause(py, Some(cause));
            py_err
        }),
        None => py_err,
    }
}

/// Build the `__cause__` for a profiling error from its retained source chain.
///
/// Each link becomes its own exception so a multi-level chain stays visible in
/// the traceback rather than being flattened into one message. Returns `None`
/// when nothing was retained, which keeps "no cause recorded" distinct from
/// "cause recorded but empty".
fn rust_cause_chain(err: &DataProfilerError) -> Option<PyErr> {
    let mut messages = Vec::new();
    let mut current = std::error::Error::source(err);
    while let Some(source) = current {
        let text = source.to_string();
        if messages.last() != Some(&text) {
            messages.push(text);
        }
        current = retained_source(source);
    }
    if messages.is_empty() {
        return None;
    }

    // Built innermost-first so the outermost cause is the error directly
    // beneath the profiling error, matching how Python renders chained causes.
    let mut cause: Option<PyErr> = None;
    for message in messages.into_iter().rev() {
        let link = PyRuntimeError::new_err(message);
        if let Some(inner) = cause.take() {
            Python::attach(|py| link.set_cause(py, Some(inner)));
        }
        cause = Some(link);
    }
    cause
}

/// Follow concrete I/O wrappers too: `csv::Error` does not expose its `Io`
/// variant through `Error::source`, and `io::Error` can skip its boxed error.
fn retained_source<'a>(
    error: &'a (dyn std::error::Error + 'static),
) -> Option<&'a (dyn std::error::Error + 'static)> {
    #[cfg(feature = "python-async")]
    if let Some(csv) = error.downcast_ref::<csv::Error>()
        && let csv::ErrorKind::Io(io) = csv.kind()
    {
        return Some(io);
    }
    if let Some(io) = error.downcast_ref::<std::io::Error>()
        && let Some(inner) = io.get_ref()
    {
        return Some(inner);
    }
    error.source()
}

/// Inspect types, never diagnostic text. A reqwest failure alone is not enough:
/// malformed HTTP, invalid requests and decode errors keep their old classes.
fn is_transport_failure(err: &DataProfilerError) -> bool {
    #[cfg(feature = "python-async")]
    {
        use std::io::ErrorKind;

        let mut saw_reqwest = false;
        let mut current = std::error::Error::source(err);
        while let Some(source) = current {
            if let Some(http) = source.downcast_ref::<reqwest::Error>() {
                if http.is_connect() || http.is_timeout() {
                    return true;
                }
                saw_reqwest = true;
            }
            if saw_reqwest
                && let Some(io) = source.downcast_ref::<std::io::Error>()
                && matches!(
                    io.kind(),
                    ErrorKind::ConnectionRefused
                        | ErrorKind::ConnectionReset
                        | ErrorKind::ConnectionAborted
                        | ErrorKind::NotConnected
                        | ErrorKind::BrokenPipe
                        | ErrorKind::TimedOut
                )
            {
                return true;
            }
            current = retained_source(source);
        }
    }
    #[cfg(not(feature = "python-async"))]
    let _ = err;
    false
}

fn analysis_error_to_py_uncaused(err: &DataProfilerError) -> PyErr {
    let message = err.to_string();
    match err {
        DataProfilerError::CsvParsingError { .. } | DataProfilerError::JsonParsingError { .. }
            if is_transport_failure(err) =>
        {
            PyIOError::new_err(message)
        }
        DataProfilerError::ParquetError { .. } if is_transport_failure(err) => {
            PyIOError::new_err(format!("Analysis failed: {message}"))
        }
        DataProfilerError::StreamingError { .. } if is_transport_failure(err) => {
            PyIOError::new_err(format!("Analysis failed: {message}"))
        }
        DataProfilerError::InvalidSemanticHint { .. }
        | DataProfilerError::InvalidConfiguration { .. }
        | DataProfilerError::DuplicateColumnName { .. }
        | DataProfilerError::JsonParsingError { .. }
        | DataProfilerError::CsvParsingError { .. }
        | DataProfilerError::EncodingError { .. }
        | DataProfilerError::BinaryInput { .. }
        | DataProfilerError::UnsupportedFormat { .. }
        | DataProfilerError::DatabaseConfigError { .. } => PyValueError::new_err(message),
        DataProfilerError::FileNotFound { .. } => PyFileNotFoundError::new_err(message),
        // A read that failed because the source changed under it: an I/O
        // condition a caller can retry, not an analysis failure (#865).
        DataProfilerError::SourceChanged { .. } => PyIOError::new_err(message),
        DataProfilerError::IoError { .. } => {
            if message.to_ascii_lowercase().contains("permission denied") {
                PyPermissionError::new_err(message)
            } else {
                PyIOError::new_err(message)
            }
        }
        DataProfilerError::DatabaseConnectionError { .. } => {
            PyIOError::new_err(format!("Analysis failed: {message}"))
        }
        _ => PyRuntimeError::new_err(format!("Analysis failed: {message}")),
    }
}

/// Map an analyzer error (an `anyhow::Error` that usually wraps a
/// [`DataProfilerError`]) onto a Python exception.
///
/// The columnar analyzers return `anyhow::Result`, which would otherwise
/// flatten every failure into a `RuntimeError`. Recovering the typed error
/// keeps input problems — notably duplicate column names — as `ValueError`, so
/// the Arrow/Parquet input paths agree with the CSV and dict/list paths.
pub(crate) fn analyzer_error_to_py(err: anyhow::Error) -> PyErr {
    match err.downcast::<DataProfilerError>() {
        Ok(typed) => analysis_error_to_py(&typed),
        Err(other) => PyRuntimeError::new_err(format!("Analysis failed: {other}")),
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

#[cfg(all(test, feature = "python-async"))]
mod tests {
    use super::*;

    #[test]
    fn a_timeout_remains_transport_through_io_and_csv_wrappers() {
        use std::net::TcpListener;
        use std::time::{Duration, Instant};

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(2);
            loop {
                match listener.accept() {
                    Ok((_connection, _)) => {
                        std::thread::sleep(Duration::from_millis(300));
                        return true;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if Instant::now() >= deadline {
                            return false;
                        }
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("loopback server failed: {error}"),
                }
            }
        });
        let timeout = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_millis(100))
                .build()
                .unwrap()
                .get(format!("http://{address}/data.csv"))
                .send()
                .await
                .unwrap_err()
        });
        assert!(timeout.is_timeout());
        let csv: csv::Error = std::io::Error::other(timeout).into();
        let error = DataProfilerError::csv_parsing_with_source("read failed", "retry", csv);
        assert!(is_transport_failure(&error));
        assert!(
            server.join().unwrap(),
            "the request must reach the loopback server"
        );
    }

    #[test]
    fn local_io_decode_errors_are_not_remote_transport() {
        let error = DataProfilerError::parquet_with_source(
            "corrupt footer",
            std::io::Error::from(std::io::ErrorKind::UnexpectedEof),
        );
        assert!(!is_transport_failure(&error));
    }
}
