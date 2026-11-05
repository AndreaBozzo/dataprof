use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::path::Path;

use dataprof::{Locale, Profiler, list_patterns as dataprof_list_patterns};

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

/// Analyze Parquet bytes held in memory and return a full profile report.
///
/// Reads through the compiled Arrow/Parquet stack, the same one the file path
/// uses, so a buffer and the file holding those bytes profile identically and
/// neither needs a third-party Python package.
///
/// The GIL is released while the buffer is decoded and profiled.
///
/// `dataprof` is depended on with default features, which include `parquet`,
/// so this is compiled into every wheel — the same reason
/// `capabilities().local_parquet` is unconditionally true.
#[pyfunction]
#[pyo3(signature = (data, name = "parquet_bytes".to_string(), max_rows = None, config = None))]
pub fn profile_parquet_bytes(
    py: Python<'_>,
    data: Vec<u8>,
    name: String,
    max_rows: Option<usize>,
    config: Option<&PyProfilerConfig>,
) -> PyResult<PyProfileReport> {
    use dataprof::{ParquetConfig, analyze_parquet_bytes_with_options};

    let options = config.map(|cfg| cfg.analysis_options()).unwrap_or_default();
    let effective_max_rows =
        max_rows.or_else(|| config.and_then(|cfg| cfg.max_rows.map(|rows| rows as usize)));

    let mut parquet_config = ParquetConfig::default();
    if let Some(cap) = effective_max_rows {
        parquet_config = parquet_config.with_max_rows(cap);
    }

    let report = py
        .detach(|| {
            analyze_parquet_bytes_with_options(data.into(), &name, &parquet_config, &options)
        })
        .map_err(|e| analysis_error_to_py(&e))?;

    Ok(PyProfileReport::new(report))
}

/// Detect the delimiter of CSV bytes with the detector every file and async
/// route uses.
///
/// Synchronous bytes are decoded in Python. Choosing the delimiter there with
/// `csv.Sniffer` let the same bytes read differently from a file, and a sample
/// the sniffer could not resolve silently became comma (#1095).
///
/// Returns the delimiter with what detection saw: the header's field count,
/// a candidate that read the sample as well, and a likely delimiter outside
/// the candidates when none split the header (#1100).
#[pyfunction]
#[pyo3(name = "_detect_csv_delimiter")]
#[allow(clippy::type_complexity)]
pub fn detect_csv_delimiter(
    data: &[u8],
) -> PyResult<(String, usize, Option<String>, Option<String>)> {
    let detection = dataprof_csv::detect_delimiter_detailed(data)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok((
        detection.delimiter,
        detection.header_fields,
        detection.runner_up,
        detection.unlisted_delimiter,
    ))
}

/// The strict-mode message the file and async readers give a malformed JSONL
/// line, or `None` when serde reads it as one JSON value.
///
/// Synchronous bytes are decoded with `json` in Python, which places most
/// faults at a different character and has no end-of-input category. Copying
/// serde's rules in Python kept missing cases, so the bytes route asks for the
/// file readers' message when `json` refuses a line (#1143).
#[pyfunction]
#[pyo3(name = "_jsonl_line_error")]
pub fn jsonl_line_error(line: &str, line_number: usize) -> Option<String> {
    dataprof_json::record::jsonl_line_error_message(line_number, line)
}

/// List supported pattern detectors and their metadata.
#[pyfunction]
#[pyo3(signature = (locale=None))]
pub fn list_patterns(
    py: Python<'_>,
    locale: Option<&str>,
) -> PyResult<Vec<HashMap<String, Py<PyAny>>>> {
    let locale = Locale::parse_optional(locale).map_err(PyValueError::new_err)?;

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
