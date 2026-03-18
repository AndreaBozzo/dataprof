//! Async Python bindings for streaming profiling.
//!
//! Feature-gated on `python-async` + `async-streaming`.
//! Provides Python-awaitable coroutines for profiling bytes, files, and URLs.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::api::Profiler;
use crate::engines::streaming::async_source::{AsyncSourceInfo, BytesSource};
use crate::types::FileFormat;

use super::config::PyProfilerConfig;
use super::partial::{PyRowCountEstimate, PySchemaResult};
use super::types::PyProfileReport;

/// Parse a format string into a FileFormat.
fn parse_format(format: &str) -> PyResult<FileFormat> {
    match format.to_lowercase().as_str() {
        "csv" => Ok(FileFormat::Csv),
        "json" => Ok(FileFormat::Json),
        "jsonl" | "ndjson" => Ok(FileFormat::Jsonl),
        "parquet" => Ok(FileFormat::Parquet),
        _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "Unknown format '{}'. Valid: csv, json, jsonl, parquet",
            format
        ))),
    }
}

/// Build a Profiler from an optional PyProfilerConfig.
fn build_profiler(config: Option<&PyProfilerConfig>) -> Profiler {
    match config {
        Some(cfg) => cfg.to_profiler(),
        None => Profiler::new(),
    }
}

/// Profile in-memory bytes asynchronously.
///
/// Accepts raw bytes and a format string. Returns a ProfileReport.
///
/// Example (Python):
///
/// ```text
/// import asyncio
/// from dataprof.asyncio import profile_bytes
///
/// async def main():
///     data = open("data.csv", "rb").read()
///     report = await profile_bytes(data, format="csv")
///     print(report)
///
/// asyncio.run(main())
/// ```
#[pyfunction]
#[pyo3(signature = (data, format, config=None))]
pub fn profile_bytes_async<'py>(
    py: Python<'py>,
    data: Vec<u8>,
    format: &str,
    config: Option<&PyProfilerConfig>,
) -> PyResult<Bound<'py, PyAny>> {
    let fmt = parse_format(format)?;
    let profiler = build_profiler(config);

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let source = BytesSource::new(
            bytes::Bytes::from(data),
            AsyncSourceInfo {
                label: "python-bytes".into(),
                format: fmt,
                size_hint: None,
                source_system: None,
                has_header: None,
            },
        );

        let report = profiler
            .profile_stream(source)
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Async profiling failed: {e}")))?;

        Ok(PyProfileReport::new(report))
    })
}

/// Profile a local file asynchronously.
///
/// All formats supported including Parquet (handled via spawn_blocking).
#[pyfunction]
#[pyo3(signature = (path, config=None))]
pub fn profile_file_async<'py>(
    py: Python<'py>,
    path: String,
    config: Option<&PyProfilerConfig>,
) -> PyResult<Bound<'py, PyAny>> {
    let profiler = build_profiler(config);

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let report = profiler
            .profile_file(&path)
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Async file profiling failed: {e}")))?;

        Ok(PyProfileReport::new(report))
    })
}

/// Infer schema from in-memory bytes asynchronously.
///
/// Reads only a small sample. Parquet is not supported (requires seeking).
#[pyfunction]
#[pyo3(signature = (data, format))]
pub fn infer_schema_stream_async<'py>(
    py: Python<'py>,
    data: Vec<u8>,
    format: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let fmt = parse_format(format)?;

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let source = BytesSource::new(
            bytes::Bytes::from(data),
            AsyncSourceInfo {
                label: "python-bytes".into(),
                format: fmt,
                size_hint: None,
                source_system: None,
                has_header: None,
            },
        );

        let result = Profiler::new()
            .infer_schema_stream(source)
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Async schema inference failed: {e}")))?;

        Ok(PySchemaResult::from_inner(result))
    })
}

/// Quick row count from in-memory bytes asynchronously.
///
/// Always a full scan (stream size is unknown). Parquet not supported.
#[pyfunction]
#[pyo3(signature = (data, format))]
pub fn quick_row_count_stream_async<'py>(
    py: Python<'py>,
    data: Vec<u8>,
    format: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let fmt = parse_format(format)?;

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let source = BytesSource::new(
            bytes::Bytes::from(data),
            AsyncSourceInfo {
                label: "python-bytes".into(),
                format: fmt,
                size_hint: None,
                source_system: None,
                has_header: None,
            },
        );

        let result = Profiler::new()
            .quick_row_count_stream(source)
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Async row count failed: {e}")))?;

        Ok(PyRowCountEstimate::from_inner(result))
    })
}

/// Profile data from a remote URL asynchronously.
///
/// Supports all formats. Parquet uses HTTP Range requests.
/// Format is auto-detected from URL extension; use `format` to override.
#[cfg(feature = "parquet-async")]
#[pyfunction]
#[pyo3(signature = (url, format=None, config=None))]
pub fn profile_url_async<'py>(
    py: Python<'py>,
    url: String,
    format: Option<&str>,
    config: Option<&PyProfilerConfig>,
) -> PyResult<Bound<'py, PyAny>> {
    let profiler = if let Some(fmt_str) = format {
        let fmt = parse_format(fmt_str)?;
        build_profiler(config).format(fmt)
    } else {
        build_profiler(config)
    };

    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let report = profiler
            .profile_url(&url)
            .await
            .map_err(|e| PyRuntimeError::new_err(format!("Async URL profiling failed: {e}")))?;

        Ok(PyProfileReport::new(report))
    })
}
