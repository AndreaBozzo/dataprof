use pyo3::prelude::*;

use super::analysis::analyze_csv_file;
// ML features removed
use super::types::PyColumnProfile;

/// Configure Python logging for DataProf
#[pyfunction]
#[pyo3(signature = (level = None, format = None))]
pub fn configure_logging(
    py: Python,
    level: Option<String>,
    format: Option<String>,
) -> PyResult<()> {
    let logging = py.import("logging")?;

    // Set default level to INFO if not specified
    let log_level = match level.as_deref() {
        Some("DEBUG") => logging.getattr("DEBUG")?,
        Some("INFO") => logging.getattr("INFO")?,
        Some("WARNING") => logging.getattr("WARNING")?,
        Some("ERROR") => logging.getattr("ERROR")?,
        Some("CRITICAL") => logging.getattr("CRITICAL")?,
        _ => logging.getattr("INFO")?, // Default level
    };

    // Set default format if not specified
    let log_format = format
        .unwrap_or_else(|| "%(asctime)s - %(name)s - %(levelname)s - %(message)s".to_string());

    // Configure basic logging
    let kwargs = pyo3::types::PyDict::new(py);
    kwargs.set_item("level", log_level)?;
    kwargs.set_item("format", log_format)?;
    logging.call_method("basicConfig", (), Some(&kwargs))?;

    Ok(())
}

/// Get a logger instance for DataProf
#[pyfunction]
#[pyo3(signature = (name = None))]
pub fn get_logger(py: Python, name: Option<String>) -> PyResult<Py<PyAny>> {
    let logging = py.import("logging")?;
    let logger_name = name.unwrap_or_else(|| "dataprof".to_string());
    let logger = logging.call_method1("getLogger", (logger_name,))?;
    Ok(logger.into())
}

/// Log a message at INFO level
#[pyfunction]
#[pyo3(signature = (message, logger_name = None))]
pub fn log_info(py: Python, message: String, logger_name: Option<String>) -> PyResult<()> {
    let logging = py.import("logging")?;
    let logger_name = logger_name.unwrap_or_else(|| "dataprof".to_string());
    let logger = logging.call_method1("getLogger", (logger_name,))?;
    logger.call_method1("info", (message,))?;
    Ok(())
}

/// Log a message at DEBUG level
#[pyfunction]
#[pyo3(signature = (message, logger_name = None))]
pub fn log_debug(py: Python, message: String, logger_name: Option<String>) -> PyResult<()> {
    let logging = py.import("logging")?;
    let logger_name = logger_name.unwrap_or_else(|| "dataprof".to_string());
    let logger = logging.call_method1("getLogger", (logger_name,))?;
    logger.call_method1("debug", (message,))?;
    Ok(())
}

/// Log a message at WARNING level
#[pyfunction]
#[pyo3(signature = (message, logger_name = None))]
pub fn log_warning(py: Python, message: String, logger_name: Option<String>) -> PyResult<()> {
    let logging = py.import("logging")?;
    let logger_name = logger_name.unwrap_or_else(|| "dataprof".to_string());
    let logger = logging.call_method1("getLogger", (logger_name,))?;
    logger.call_method1("warning", (message,))?;
    Ok(())
}

/// Log a message at ERROR level
#[pyfunction]
#[pyo3(signature = (message, logger_name = None))]
pub fn log_error(py: Python, message: String, logger_name: Option<String>) -> PyResult<()> {
    let logging = py.import("logging")?;
    let logger_name = logger_name.unwrap_or_else(|| "dataprof".to_string());
    let logger = logging.call_method1("getLogger", (logger_name,))?;
    logger.call_method1("error", (message,))?;
    Ok(())
}

/// Enhanced CSV analysis with logging
#[pyfunction]
#[pyo3(signature = (file_path, log_level = None))]
pub fn analyze_csv_with_logging(
    py: Python,
    file_path: String,
    log_level: Option<String>,
) -> PyResult<Vec<PyColumnProfile>> {
    // Configure logging if level is provided
    if let Some(level) = log_level {
        configure_logging(py, Some(level), None)?;
    }

    log_info(
        py,
        format!("Starting CSV analysis for: {}", file_path),
        None,
    )?;

    // Perform the actual analysis
    let start_time = std::time::Instant::now();
    let result = analyze_csv_file(&file_path, None);
    let duration = start_time.elapsed();

    match result {
        Ok(profiles) => {
            log_info(
                py,
                format!(
                    "CSV analysis completed in {:.3}s. Analyzed {} columns",
                    duration.as_secs_f64(),
                    profiles.len()
                ),
                None,
            )?;
            Ok(profiles)
        }
        Err(e) => {
            log_error(
                py,
                format!("CSV analysis failed for {}: {}", file_path, e),
                None,
            )?;
            Err(e)
        }
    }
}
