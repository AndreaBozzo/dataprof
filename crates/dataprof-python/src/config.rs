use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use std::sync::Arc;
use std::time::Duration;

use dataprof::{
    ChunkSize, EngineType, FileFormat, MetricPack, Profiler, QualityDimension, SemanticHints,
    StopCondition,
};

use super::progress::py_callback_to_sink;
use super::sampling::PySamplingStrategy;
use super::stop_condition::PyStopCondition;

/// Python-friendly profiler configuration.
///
/// Maps Python kwargs to Rust `ProfilerConfig` fields with validation.
#[pyclass(name = "ProfilerConfig", from_py_object)]
#[derive(Clone)]
pub struct PyProfilerConfig {
    pub(crate) engine: EngineType,
    pub(crate) chunk_size: Option<usize>,
    pub(crate) memory_limit_mb: Option<usize>,
    pub(crate) format_override: Option<FileFormat>,
    pub(crate) max_rows: Option<u64>,
    pub(crate) csv_delimiter: Option<u8>,
    pub(crate) csv_flexible: Option<bool>,
    pub(crate) sampling: Option<PySamplingStrategy>,
    pub(crate) stop_condition: Option<PyStopCondition>,
    pub(crate) on_progress: Option<Arc<Py<PyAny>>>,
    pub(crate) progress_interval_ms: Option<u64>,
    pub(crate) quality_dimensions: Option<Vec<QualityDimension>>,
    pub(crate) metric_packs: Option<Vec<MetricPack>>,
    pub(crate) locale: Option<String>,
    pub(crate) positive_columns: Vec<String>,
    pub(crate) identifier_columns: Vec<String>,
}

#[pymethods]
impl PyProfilerConfig {
    #[new]
    #[pyo3(signature = (
        engine = "auto",
        chunk_size = None,
        memory_limit_mb = None,
        format = None,
        max_rows = None,
        csv_delimiter = None,
        csv_flexible = None,
        sampling = None,
        stop_condition = None,
        on_progress = None,
        progress_interval_ms = None,
        quality_dimensions = None,
        metrics = None,
        locale = None,
        positive_columns = None,
        identifier_columns = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        engine: &str,
        chunk_size: Option<usize>,
        memory_limit_mb: Option<usize>,
        format: Option<&str>,
        max_rows: Option<u64>,
        csv_delimiter: Option<&str>,
        csv_flexible: Option<bool>,
        sampling: Option<PySamplingStrategy>,
        stop_condition: Option<PyStopCondition>,
        on_progress: Option<Py<PyAny>>,
        progress_interval_ms: Option<u64>,
        quality_dimensions: Option<Vec<String>>,
        metrics: Option<Vec<String>>,
        locale: Option<String>,
        positive_columns: Option<Vec<String>>,
        identifier_columns: Option<Vec<String>>,
    ) -> PyResult<Self> {
        let engine = parse_engine(engine)?;
        let format_override = format.map(parse_format).transpose()?;
        let csv_delimiter = csv_delimiter
            .map(|d| {
                if d.len() != 1 {
                    return Err(PyValueError::new_err(
                        "csv_delimiter must be a single character",
                    ));
                }
                Ok(d.as_bytes()[0])
            })
            .transpose()?;

        if max_rows.is_some() && stop_condition.is_some() {
            return Err(PyValueError::new_err(
                "Cannot specify both max_rows and stop_condition. \
                 Use StopCondition.max_rows() within a composed stop_condition instead.",
            ));
        }

        if let Some(ref cb) = on_progress {
            Python::attach(|py| -> PyResult<()> {
                if !cb.bind(py).is_callable() {
                    return Err(pyo3::exceptions::PyTypeError::new_err(
                        "on_progress must be a callable (e.g., a function or lambda)",
                    ));
                }
                Ok(())
            })?;
        }

        // Parse quality_dimensions strings into QualityDimension enums
        let quality_dimensions = quality_dimensions
            .map(|dims| {
                dims.iter()
                    .map(|s| s.parse::<QualityDimension>().map_err(PyValueError::new_err))
                    .collect::<PyResult<Vec<_>>>()
            })
            .transpose()?;

        // Parse metrics strings into MetricPack enums
        let metric_packs = metrics
            .map(|packs| {
                packs
                    .iter()
                    .map(|s| s.parse::<MetricPack>().map_err(PyValueError::new_err))
                    .collect::<PyResult<Vec<_>>>()
            })
            .transpose()?;

        Ok(Self {
            engine,
            chunk_size,
            memory_limit_mb,
            format_override,
            max_rows,
            csv_delimiter,
            csv_flexible,
            sampling,
            stop_condition,
            on_progress: on_progress.map(Arc::new),
            progress_interval_ms,
            quality_dimensions,
            metric_packs,
            locale,
            positive_columns: positive_columns.unwrap_or_default(),
            identifier_columns: identifier_columns.unwrap_or_default(),
        })
    }

    /// Engine type as string
    #[getter]
    fn engine(&self) -> &str {
        match self.engine {
            EngineType::Auto => "auto",
            EngineType::Incremental => "incremental",
            EngineType::Columnar => "columnar",
        }
    }

    /// Chunk size (None = adaptive)
    #[getter]
    fn chunk_size(&self) -> Option<usize> {
        self.chunk_size
    }

    /// Memory limit in MB
    #[getter]
    fn memory_limit_mb(&self) -> Option<usize> {
        self.memory_limit_mb
    }

    /// Format override
    #[getter]
    fn format(&self) -> Option<String> {
        self.format_override.as_ref().map(|f| f.to_string())
    }

    /// Maximum rows to process
    #[getter]
    fn max_rows(&self) -> Option<u64> {
        self.max_rows
    }

    /// CSV delimiter as a one-character string
    #[getter]
    fn csv_delimiter(&self) -> Option<String> {
        self.csv_delimiter.map(|d| (d as char).to_string())
    }

    /// Whether CSV parsing is flexible on ragged rows
    #[getter]
    fn csv_flexible(&self) -> Option<bool> {
        self.csv_flexible
    }

    /// Locale for pattern detection
    #[getter]
    fn locale(&self) -> Option<&str> {
        self.locale.as_deref()
    }

    /// Columns expected to contain non-negative numeric values
    #[getter]
    fn positive_columns(&self) -> Vec<String> {
        self.positive_columns.clone()
    }

    /// Columns that should be treated as semantic identifiers
    #[getter]
    fn identifier_columns(&self) -> Vec<String> {
        self.identifier_columns.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "ProfilerConfig(engine='{}', chunk_size={}, memory_limit_mb={}, format={}, max_rows={})",
            self.engine(),
            self.chunk_size
                .map(|s| s.to_string())
                .unwrap_or("None".into()),
            self.memory_limit_mb
                .map(|s| s.to_string())
                .unwrap_or("None".into()),
            self.format()
                .map(|s| format!("'{}'", s))
                .unwrap_or("None".into()),
            self.max_rows
                .map(|s| s.to_string())
                .unwrap_or("None".into()),
        )
    }
}

impl PyProfilerConfig {
    /// Build a configured `Profiler` from this config.
    pub(crate) fn to_profiler(&self) -> Profiler {
        let mut profiler = Profiler::new().engine(self.engine);

        if let Some(n) = self.chunk_size {
            profiler = profiler.chunk_size(ChunkSize::Fixed(n));
        }
        if let Some(mb) = self.memory_limit_mb {
            profiler = profiler.memory_limit_mb(mb);
        }
        if let Some(ref fmt) = self.format_override {
            profiler = profiler.format(fmt.clone());
        }
        if let Some(max) = self.max_rows {
            profiler = profiler.stop_when(StopCondition::MaxRows(max));
        }
        if let Some(ref sc) = self.stop_condition {
            profiler = profiler.stop_when(sc.clone().into_inner());
        }
        if let Some(ref s) = self.sampling {
            profiler = profiler.sampling(s.clone().into_inner());
        }
        if let Some(d) = self.csv_delimiter {
            profiler = profiler.csv_delimiter(d);
        }
        if let Some(f) = self.csv_flexible {
            profiler = profiler.csv_flexible(f);
        }
        if let Some(ref cb) = self.on_progress {
            profiler = profiler.progress_sink(py_callback_to_sink(Arc::clone(cb)));
        }
        if let Some(ms) = self.progress_interval_ms {
            profiler = profiler.progress_interval(Duration::from_millis(ms));
        }
        if let Some(ref dims) = self.quality_dimensions {
            profiler = profiler.quality_dimensions(dims.clone());
        }
        if let Some(ref packs) = self.metric_packs {
            profiler = profiler.metric_packs(packs.clone());
        }
        if let Some(ref l) = self.locale {
            profiler = profiler.locale(l);
        }
        if !self.positive_columns.is_empty() {
            profiler = profiler.positive_columns(self.positive_columns.clone());
        }
        if !self.identifier_columns.is_empty() {
            profiler = profiler.identifier_columns(self.identifier_columns.clone());
        }

        profiler
    }

    pub(crate) fn semantic_hints(&self) -> SemanticHints {
        SemanticHints::new(
            self.positive_columns.clone(),
            self.identifier_columns.clone(),
        )
    }
}

fn parse_engine(s: &str) -> PyResult<EngineType> {
    match s.to_lowercase().as_str() {
        "auto" => Ok(EngineType::Auto),
        "incremental" | "streaming" => Ok(EngineType::Incremental),
        "columnar" | "arrow" => Ok(EngineType::Columnar),
        _ => Err(PyValueError::new_err(format!(
            "Unknown engine '{}'. Valid: auto, incremental, columnar",
            s
        ))),
    }
}

fn parse_format(s: &str) -> PyResult<FileFormat> {
    match s.to_lowercase().as_str() {
        "csv" => Ok(FileFormat::Csv),
        "json" => Ok(FileFormat::Json),
        "jsonl" | "ndjson" => Ok(FileFormat::Jsonl),
        "parquet" => Ok(FileFormat::Parquet),
        _ => Err(PyValueError::new_err(format!(
            "Unknown format '{}'. Valid: csv, json, jsonl, parquet",
            s
        ))),
    }
}
